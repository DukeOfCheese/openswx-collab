//! Load generator for the collaboration server. See bench/README.md.

mod dialect;
mod doc;
mod report;
mod scenarios;
mod v1;
mod v2;

use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, bail};
use clap::{Parser, Subcommand, ValueEnum};
use serde_json::json;
use sysinfo::{Pid, ProcessesToUpdate, System};

use crate::dialect::Dialect;
use crate::report::{Host, LATENCY_NOTE, Report, WelcomeBytes};
use crate::scenarios::RunResult;

#[derive(Copy, Clone, ValueEnum)]
enum DialectName {
    /// The Python server: JSON frames, protocol 1.
    V1,
    /// The Rust server: MessagePack frames, protocol 2.
    V2,
}

#[derive(Parser)]
#[command(
    name = "loadgen",
    about = "Load generator for the collaboration server"
)]
struct Cli {
    #[arg(long, value_enum, global = true, default_value = "v1")]
    dialect: DialectName,
    /// Base URL of the server.
    #[arg(long, global = true, default_value = "http://127.0.0.1:8791")]
    url: String,
    /// How long to run (`30s`, `2m`, `500ms`).
    #[arg(long, global = true, default_value = "30s", value_parser = parse_duration)]
    duration: Duration,
    /// Write the JSON report here (it is always printed too).
    #[arg(long, global = true)]
    out: Option<std::path::PathBuf>,
    /// Free text stored in the report.
    #[arg(long, global = true)]
    note: Option<String>,
    /// Which database the server runs on (stored in the report; loadgen cannot see it).
    #[arg(long, global = true)]
    database: Option<String>,
    /// Process id of the server, to report its memory (idle scenario).
    #[arg(long, global = true)]
    pid: Option<u32>,
    #[command(subcommand)]
    scenario: Scenario,
}

#[derive(Subcommand)]
enum Scenario {
    /// N connections on one document; each edits its own feature.
    OneDoc {
        #[arg(long, default_value_t = 4)]
        editors: usize,
        /// Edits per second, per editor.
        #[arg(long, default_value_t = 5.0)]
        rate: f64,
        #[arg(long, default_value_t = 20)]
        features: usize,
    },
    /// D documents, E editors each.
    ManyDocs {
        #[arg(long, default_value_t = 10)]
        docs: usize,
        #[arg(long, default_value_t = 4)]
        editors_per_doc: usize,
        #[arg(long, default_value_t = 5.0)]
        rate: f64,
        #[arg(long, default_value_t = 20)]
        features: usize,
    },
    /// Time from hello to welcome for a document of about B bytes.
    Open {
        #[arg(long, default_value_t = 1_000_000)]
        doc_bytes: usize,
        #[arg(long, default_value_t = 10)]
        times: usize,
        #[arg(long, default_value_t = 100)]
        features: usize,
    },
    /// N connections sending only pings and presence.
    Idle {
        #[arg(long, default_value_t = 100)]
        connections: usize,
    },
}

fn parse_duration(s: &str) -> Result<Duration, String> {
    let (num, unit) = s.split_at(
        s.find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(s.len()),
    );
    let n: f64 = num.parse().map_err(|_| format!("not a duration: {s}"))?;
    let secs = match unit {
        "ms" => n / 1000.0,
        "s" | "" => n,
        "m" => n * 60.0,
        _ => return Err(format!("not a duration: {s} (use 500ms, 30s or 2m)")),
    };
    if !secs.is_finite() || secs < 0.0 {
        return Err(format!("not a duration: {s}"));
    }
    Ok(Duration::from_secs_f64(secs))
}

fn host_info() -> Host {
    let mut sys = System::new();
    sys.refresh_cpu_all();
    sys.refresh_memory();
    Host {
        os: System::long_os_version().unwrap_or_else(|| std::env::consts::OS.to_owned()),
        cpu: sys
            .cpus()
            .first()
            .map(|c| c.brand().trim().to_owned())
            .unwrap_or_default(),
        cpu_threads: sys.cpus().len(),
        ram_bytes: sys.total_memory(),
    }
}

fn process_rss(pid: u32) -> Option<u64> {
    let pid = Pid::from_u32(pid);
    let mut sys = System::new();
    sys.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    sys.process(pid).map(|p| p.memory())
}

fn git_commit() -> Option<String> {
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    let s = String::from_utf8(out.stdout).ok()?;
    (out.status.success() && !s.trim().is_empty()).then(|| s.trim().to_owned())
}

fn positive(name: &str, n: usize) -> Result<()> {
    if n == 0 {
        bail!("--{name} must be at least 1")
    } else {
        Ok(())
    }
}

async fn run(cli: Cli) -> Result<Report> {
    let dialect: Arc<dyn Dialect> = match cli.dialect {
        DialectName::V1 => Arc::new(v1::V1::new()),
        DialectName::V2 => Arc::new(v2::V2::new()),
    };
    let started_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);

    let (name, params, kind, target, result): (&str, serde_json::Value, &str, _, RunResult) =
        match cli.scenario {
            Scenario::OneDoc {
                editors,
                rate,
                features,
            } => {
                positive("editors", editors)?;
                if !(rate.is_finite() && rate > 0.0) {
                    bail!("--rate must be positive");
                }
                let target = dialect.setup(&cli.url, 1, features, 0).await?;
                let r = scenarios::edit_run(
                    dialect.clone(),
                    target.clone(),
                    editors,
                    rate,
                    cli.duration,
                )
                .await;
                let p = json!({"editors": editors, "rate": rate, "features": features});
                ("one-doc", p, "edit", target, r)
            }
            Scenario::ManyDocs {
                docs,
                editors_per_doc,
                rate,
                features,
            } => {
                positive("docs", docs)?;
                positive("editors-per-doc", editors_per_doc)?;
                if !(rate.is_finite() && rate > 0.0) {
                    bail!("--rate must be positive");
                }
                let target = dialect.setup(&cli.url, docs, features, 0).await?;
                let r = scenarios::edit_run(
                    dialect.clone(),
                    target.clone(),
                    docs * editors_per_doc,
                    rate,
                    cli.duration,
                )
                .await;
                let p = json!({"docs": docs, "editors_per_doc": editors_per_doc, "rate": rate, "features": features});
                ("many-docs", p, "edit", target, r)
            }
            Scenario::Open {
                doc_bytes,
                times,
                features,
            } => {
                positive("times", times)?;
                let target = dialect.setup(&cli.url, 1, features, doc_bytes).await?;
                let r = scenarios::open_run(dialect.clone(), target.clone(), times).await?;
                let p = json!({"doc_bytes": doc_bytes, "times": times, "features": features});
                ("open", p, "open", target, r)
            }
            Scenario::Idle { connections } => {
                positive("connections", connections)?;
                let target = dialect.setup(&cli.url, 1, 1, 0).await?;
                let pid = cli.pid;
                let sample = move || pid.and_then(process_rss);
                let r = scenarios::idle_run(
                    dialect.clone(),
                    target.clone(),
                    connections,
                    cli.duration,
                    &sample,
                )
                .await;
                (
                    "idle",
                    json!({"connections": connections, "pid": cli.pid}),
                    "none",
                    target,
                    r,
                )
            }
        };

    let m = &result.metrics;
    if m.sent != m.accepted + m.rejected_total() + m.timeouts + m.errors {
        eprintln!("warning: the counters do not add up (this is a loadgen bug)");
    }
    let secs = result.elapsed.as_secs_f64().max(f64::EPSILON);
    Ok(Report {
        scenario: name.to_owned(),
        params,
        dialect: dialect.name().to_owned(),
        server: target.server.clone(),
        database: cli.database,
        loadgen_commit: git_commit(),
        started_at,
        note: cli.note,
        host: host_info(),
        duration_s: result.elapsed.as_secs_f64(),
        document_bytes: target.document_bytes,
        changes_sent: m.sent,
        accepted: m.accepted,
        rejected: m.rejected.clone(),
        timeouts: m.timeouts,
        errors: m.errors,
        protocol_errors: m.protocol_errors,
        dropped_connections: m.dropped,
        close_codes: dialect.closes(),
        connect_failed: m.connect_failed,
        latency_kind: kind.to_owned(),
        latency_ms: m.latency(),
        latency_from_schedule_ms: m.latency_from_schedule(),
        latency_note: LATENCY_NOTE.to_owned(),
        first_open_ms: m.first_open.map(|d| d.as_secs_f64() * 1000.0),
        welcome_bytes: m.welcome_bytes.map(|(min, max)| WelcomeBytes { min, max }),
        accepted_per_s: m.accepted as f64 / secs,
        server_rss_bytes: result.rss,
    })
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    let out = cli.out.clone();
    let outcome = async {
        let report = run(cli).await?;
        let text = serde_json::to_string_pretty(&report)?;
        if let Some(path) = out {
            std::fs::write(&path, &text).with_context(|| format!("writing {}", path.display()))?;
        }
        println!("{text}");
        anyhow::Ok(())
    }
    .await;
    match outcome {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations() {
        assert_eq!(parse_duration("30s"), Ok(Duration::from_secs(30)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("500ms"), Ok(Duration::from_millis(500)));
        assert!(parse_duration("soon").is_err());
        assert!(parse_duration("10h").is_err());
        assert!(parse_duration("").is_err());
    }
}
