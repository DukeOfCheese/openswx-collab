//! The program's panic hook: where a panic was is logged; what it said never is.
//!
//! A panic hook is the process's, so the panics happen in a process of their own: this test
//! binary run again, for the one test below that panics.

use std::process::Command;

const SECRET: &str = "the-contents-of-a-document-that-must-not-be-logged";
const HOOK_ENV: &str = "OSWX_TEST_PANIC_HOOK";

/// Run by the tests below in a process of its own: panics, with a message that holds a secret, on
/// a thread of its own, in a task, and on the test's thread.
#[test]
#[ignore = "run by the tests of this file, in a process of its own"]
fn panic_with_a_secret_in_the_message() {
    let Ok(hook) = std::env::var(HOOK_ENV) else {
        return;
    };
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_ansi(false)
        .init();
    if hook == "the program's" {
        oswx_serverd::log_panics_without_their_messages();
    }
    let on_a_thread = std::thread::Builder::new()
        .name("worker".into())
        .spawn(|| panic!("on a thread: {SECRET}"))
        .unwrap();
    assert!(on_a_thread.join().is_err());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let in_a_task = runtime.block_on(async {
        let task = tokio::spawn(async {
            let content = String::from(SECRET);
            panic!("in a task: {content}");
        });
        task.await
    });
    assert!(in_a_task.is_err());
    std::panic::panic_any(format!("a payload that is a String: {SECRET}"));
}

/// Everything the process wrote when it ran the panicking test with `hook`.
fn panicking_process_wrote(hook: &str) -> String {
    let output = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "panic_with_a_secret_in_the_message",
            "--ignored",
            "--nocapture",
        ])
        .env(HOOK_ENV, hook)
        .env_remove("RUST_BACKTRACE")
        .output()
        .expect("the test binary runs again");
    assert!(!output.status.success(), "the panicking test passed");
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

#[test]
fn a_panic_is_logged_with_its_place_and_without_its_message() {
    let wrote = panicking_process_wrote("the program's");
    assert!(!wrote.contains(SECRET), "the message was written: {wrote}");
    assert!(
        !wrote.contains("on a thread:") && !wrote.contains("in a task:"),
        "{wrote}"
    );
    // Three panics, each with the file and line it was at, and the thread.
    let logged: Vec<&str> = wrote
        .lines()
        .filter(|l| l.contains("a panic (its message is withheld)"))
        .collect();
    assert_eq!(logged.len(), 3, "{wrote}");
    for line in &logged {
        assert!(line.contains("ERROR"), "{line}");
        assert!(
            line.contains("panic_hook.rs") && line.contains("line="),
            "{line}"
        );
    }
    assert!(logged[0].contains("thread=\"worker\""), "{}", logged[0]);
}

/// The check on the test above: Rust's own hook does write the message, so the test would see it.
#[test]
fn without_the_hook_the_message_would_be_written() {
    let wrote = panicking_process_wrote("Rust's");
    assert!(wrote.contains(SECRET), "{wrote}");
}
