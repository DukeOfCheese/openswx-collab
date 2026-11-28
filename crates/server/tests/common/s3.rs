//! S3 for the tests: an S3-compatible server inside the test process ([`FakeS3`]: the `s3s-fs`
//! crate on a free local port, with every request passing a gate that can be told to fail, hang or
//! stall), and the real endpoint `OSWX_TEST_S3_URL` names, if any ([`real_s3`]).

use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::{HeaderValue, StatusCode, header};
use axum::response::{IntoResponse, Response};
use bytes::Bytes;
use futures_util::StreamExt;
use oswx_server::blobs::{S3Config, S3Tuning};
use s3s::auth::SimpleAuth;
use s3s::service::{S3Service, S3ServiceBuilder};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

/// The credentials [`FakeS3`] takes.
pub const FAKE_ACCESS_KEY: &str = "AKIAFAKEFAKEFAKEFAKE";
pub const FAKE_SECRET_KEY: &str = "fake/secret+key/that/must/never/be/printed";
/// The bucket [`FakeS3`] starts with.
pub const FAKE_BUCKET: &str = "collab-test";

/// One request the fake saw.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Seen {
    pub method: String,
    pub path: String,
    pub query: String,
}

impl Seen {
    /// `HEAD` of an object.
    pub fn is_head(&self) -> bool {
        self.method == "HEAD"
    }

    /// `PUT ...?partNumber=..`: one part of a multipart upload.
    pub fn is_part(&self) -> bool {
        self.method == "PUT" && self.query.contains("partNumber=")
    }

    /// `POST ...?uploads`: a multipart upload being started.
    pub fn starts_upload(&self) -> bool {
        self.method == "POST"
            && self
                .query
                .split('&')
                .any(|q| q == "uploads" || q == "uploads=")
    }

    /// `POST ...?uploadId=..`: a multipart upload being completed.
    pub fn completes_upload(&self) -> bool {
        self.method == "POST" && self.query.contains("uploadId=")
    }

    /// `DELETE ...?uploadId=..`: a multipart upload being aborted.
    pub fn aborts_upload(&self) -> bool {
        self.method == "DELETE" && self.query.contains("uploadId=")
    }

    /// `GET` of an object (not a listing).
    pub fn is_get(&self) -> bool {
        self.method == "GET" && !self.query.contains("list-type=")
    }

    /// `GET ...?list-type=2`: a page of a listing.
    pub fn is_list(&self) -> bool {
        self.method == "GET" && self.query.contains("list-type=")
    }

    /// `POST ...?delete`: a batch of deletes.
    pub fn is_batch_delete(&self) -> bool {
        self.method == "POST"
            && self
                .query
                .split('&')
                .any(|q| q == "delete" || q == "delete=")
    }
}

/// What the fake does to a request instead of serving it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Fault {
    /// Answers this status with an S3 error body (after reading the request).
    Status(u16),
    /// Never answers.
    Hang,
    /// Answers only once [`FakeS3::release`] has been called (and is served then).
    Hold,
    /// Serves it, but the answer's body stops (without ending) after this many bytes.
    StallBodyAfter(usize),
}

type When = Box<dyn Fn(&Seen) -> bool + Send + Sync>;

struct Rule {
    left: usize,
    fault: Fault,
    when: When,
}

struct Gate {
    service: S3Service,
    /// Where the server keeps its buckets, one folder each.
    root: PathBuf,
    rules: Mutex<Vec<Rule>>,
    seen: Mutex<Vec<Seen>>,
    /// Becomes true when the requests held by [`Fault::Hold`] are let go.
    released: tokio::sync::watch::Sender<bool>,
}

impl Gate {
    fn fault_for(&self, seen: &Seen) -> Option<Fault> {
        let mut rules = self.rules.lock().unwrap_or_else(PoisonError::into_inner);
        let at = rules.iter().position(|r| r.left > 0 && (r.when)(seen))?;
        rules[at].left -= 1;
        Some(rules[at].fault)
    }
}

/// An S3-compatible server in this process, keeping its objects in a folder. Dropping it stops it.
pub struct FakeS3 {
    /// `http://127.0.0.1:<port>`.
    pub endpoint: String,
    root: PathBuf,
    gate: Arc<Gate>,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl FakeS3 {
    /// Starts one that keeps its objects under `dir/s3`, with the bucket [`FAKE_BUCKET`].
    pub async fn start(dir: &Path) -> Self {
        let root = dir.join("s3");
        std::fs::create_dir_all(root.join(FAKE_BUCKET)).expect("the fake bucket's folder");
        let fs = s3s_fs::FileSystem::new(&root).expect("the fake S3's folder");
        let mut builder = S3ServiceBuilder::new(fs);
        builder.set_auth(SimpleAuth::from_single(FAKE_ACCESS_KEY, FAKE_SECRET_KEY));
        let gate = Arc::new(Gate {
            service: builder.build(),
            root: root.clone(),
            rules: Mutex::new(Vec::new()),
            seen: Mutex::new(Vec::new()),
            released: tokio::sync::watch::Sender::new(false),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("a free port");
        let endpoint = format!("http://{}", listener.local_addr().expect("an address"));
        let app = axum::Router::new().fallback(serve).with_state(gate.clone());
        let (stop, stopped) = oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            // Connections are cut when the server is told to stop, not waited for (a hung
            // request would never end): the serving future is simply dropped.
            tokio::select! {
                served = axum::serve(listener, app) => served.expect("the fake S3 runs"),
                _ = stopped => {}
            }
        });
        Self {
            endpoint,
            root,
            gate,
            stop: Mutex::new(Some(stop)),
            task: Mutex::new(Some(task)),
        }
    }

    /// How to reach it, with everything under `prefix` (empty for none): short waits between
    /// retries, the server's own timeouts and sizes otherwise.
    pub fn config(&self, prefix: &str) -> S3Config {
        S3Config {
            bucket: FAKE_BUCKET.into(),
            prefix: prefix.into(),
            endpoint: self.endpoint.clone(),
            region: "us-east-1".into(),
            access_key_id: FAKE_ACCESS_KEY.into(),
            secret_access_key: FAKE_SECRET_KEY.into(),
            session_token: String::new(),
            path_style: true,
            tuning: test_tuning(),
        }
    }

    /// The next `times` requests that `when` holds for get `fault` instead of being served.
    pub fn inject(
        &self,
        times: usize,
        fault: Fault,
        when: impl Fn(&Seen) -> bool + Send + Sync + 'static,
    ) {
        self.gate
            .rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(Rule {
                left: times,
                fault,
                when: Box::new(when),
            });
    }

    /// Lets go of every request held by [`Fault::Hold`], and of those it holds from now on.
    pub fn release(&self) {
        self.gate.released.send_replace(true);
    }

    /// Forgets every fault not yet used up.
    pub fn heal(&self) {
        self.gate
            .rules
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    /// Every request seen so far, in order.
    pub fn seen(&self) -> Vec<Seen> {
        self.gate
            .seen
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// How many requests seen so far `which` holds for.
    pub fn count(&self, which: impl Fn(&Seen) -> bool) -> usize {
        self.seen().iter().filter(|s| which(s)).count()
    }

    /// How many parts of multipart uploads that were neither completed nor aborted the server
    /// holds.
    pub fn parts_held(&self) -> usize {
        std::fs::read_dir(&self.root)
            .map(|entries| {
                entries
                    .flatten()
                    .filter(|e| e.file_name().to_string_lossy().starts_with(".upload_id-"))
                    .count()
            })
            .unwrap_or(0)
    }

    /// The file an object of the bucket is kept in.
    pub fn object_file(&self, key: &str) -> PathBuf {
        key.split('/')
            .fold(self.root.join(FAKE_BUCKET), |p, c| p.join(c))
    }

    /// Removes the bucket with everything in it, as if it had been deleted at the provider.
    pub fn remove_bucket(&self) {
        std::fs::remove_dir_all(self.root.join(FAKE_BUCKET)).expect("the bucket is removed");
    }

    /// Stops the server and cuts its connections: the endpoint is unreachable from now on.
    pub async fn stop(&self) {
        let stop = self
            .stop
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(stop) = stop {
            let _ = stop.send(());
        }
        let task = self
            .task
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(task) = task {
            let _ = task.await;
        }
    }
}

impl Drop for FakeS3 {
    fn drop(&mut self) {
        if let Some(task) = self
            .task
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            task.abort();
        }
    }
}

/// The server's S3 tuning with waits between retries short enough for tests.
pub fn test_tuning() -> S3Tuning {
    S3Tuning {
        backoff_init: Duration::from_millis(10),
        backoff_max: Duration::from_millis(50),
        ..S3Tuning::default()
    }
}

fn s3_error(status: StatusCode) -> Response {
    let code = match status.as_u16() {
        403 => "AccessDenied",
        404 => "NoSuchBucket",
        429 | 503 => "SlowDown",
        _ => "InternalError",
    };
    let body = format!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?><Error><Code>{code}</Code>\
         <Message>the fake S3 was told to fail</Message>\
         <AWSAccessKeyId>{FAKE_ACCESS_KEY}</AWSAccessKeyId></Error>"
    );
    let mut response = (status, body).into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("application/xml"),
    );
    response
}

async fn serve(State(gate): State<Arc<Gate>>, request: Request) -> Response {
    let seen = Seen {
        method: request.method().as_str().to_owned(),
        path: request.uri().path().to_owned(),
        query: request.uri().query().unwrap_or("").to_owned(),
    };
    gate.seen
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .push(seen.clone());
    let fault = gate.fault_for(&seen);
    match fault {
        Some(Fault::Status(code)) => {
            // (Read first, so that the client gets the answer rather than a cut connection.)
            let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
            return s3_error(StatusCode::from_u16(code).expect("a status"));
        }
        Some(Fault::Hang) => {
            std::future::pending::<()>().await;
        }
        Some(Fault::Hold) => {
            let mut released = gate.released.subscribe();
            let _ = released.wait_for(|&r| r).await;
        }
        _ => {}
    }
    // A bucket that is not there is 404 for everything, as at a real store (`s3s-fs` would make
    // the bucket on the first write).
    let bucket = seen.path.trim_start_matches('/').split('/').next();
    if !bucket.is_some_and(|b| !b.is_empty() && gate.root.join(b).is_dir()) {
        let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
        return s3_error(StatusCode::NOT_FOUND);
    }
    let request = request.map(s3s::Body::http_body_unsync);
    let response = match gate.service.call(request).await {
        Ok(response) => response.map(Body::new),
        Err(_) => return s3_error(StatusCode::INTERNAL_SERVER_ERROR),
    };
    match fault {
        Some(Fault::StallBodyAfter(limit)) => response.map(|body| stall_after(body, limit)),
        _ => response,
    }
}

/// `body`, which stops after `limit` bytes and then neither ends nor fails.
fn stall_after(body: Body, limit: usize) -> Body {
    let chunks = body.into_data_stream();
    let cut =
        futures_util::stream::unfold((chunks, 0usize), move |(mut chunks, sent)| async move {
            if sent >= limit {
                std::future::pending::<()>().await;
            }
            let chunk: Bytes = chunks.next().await?.ok()?;
            let take = chunk.len().min(limit - sent);
            Some((
                Ok::<_, std::io::Error>(chunk.slice(..take)),
                (chunks, sent + take),
            ))
        });
    Body::from_stream(cut)
}

// ------------------------------------------------------------------ a real endpoint

/// The environment variable that names a real S3 endpoint to run the storage tests against
/// (`http://127.0.0.1:3900` for a local Garage). With it go `OSWX_TEST_S3_BUCKET` (which must
/// exist; `oswx-test` if unset), `OSWX_TEST_S3_ACCESS_KEY`, `OSWX_TEST_S3_SECRET_KEY`,
/// `OSWX_TEST_S3_REGION` (`us-east-1` if unset) and `OSWX_TEST_S3_PATH_STYLE` (`false` puts the
/// bucket in the host name; in the path if unset).
pub const S3_ENV: &str = "OSWX_TEST_S3_URL";

fn env(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|v| !v.trim().is_empty())
}

/// How many multipart uploads that were neither completed nor aborted the bucket holds under
/// `config.prefix`: a signed `ListMultipartUploads` (the storage's own S3 client has no call for
/// it). For a real endpoint; the fake does not implement the listing ([`FakeS3::parts_held`]
/// looks at its files instead). At most the first page (1,000 uploads) is counted.
pub async fn unfinished_uploads(config: &S3Config) -> Result<usize, String> {
    let response = list_uploads(config).await?;
    let status = response.status();
    let body = response.text().await.map_err(|e| e.to_string())?;
    if !status.is_success() {
        return Err(format!("ListMultipartUploads answered {status}"));
    }
    Ok(body.matches("<Upload>").count())
}

/// Sends a signed `ListMultipartUploads` for `config`'s bucket and prefix.
pub async fn list_uploads(config: &S3Config) -> Result<reqwest::Response, String> {
    use object_store::aws::{AwsAuthorizer, AwsCredential};
    use object_store::client::{HttpRequest, HttpRequestBody};

    let bucket = if config.path_style {
        format!("{}/{}", config.endpoint, config.bucket)
    } else {
        match config.endpoint.split_once("://") {
            Some((scheme, rest)) => format!("{scheme}://{}.{rest}/", config.bucket),
            None => return Err("the endpoint has no scheme".into()),
        }
    };
    let mut url = url::Url::parse(&bucket).map_err(|e| e.to_string())?;
    let prefix = if config.prefix.is_empty() {
        String::new()
    } else {
        format!("{}/", config.prefix)
    };
    url.query_pairs_mut()
        .append_pair("prefix", &prefix)
        .append_pair("uploads", "");
    let credential = AwsCredential {
        key_id: config.access_key_id.clone(),
        secret_key: config.secret_access_key.clone(),
        token: None,
    };
    let mut signed: HttpRequest = axum::http::Request::builder()
        .method("GET")
        .uri(url.as_str())
        .body(HttpRequestBody::empty())
        .map_err(|e| e.to_string())?;
    AwsAuthorizer::new(&credential, "s3", &config.region)
        .try_authorize(&mut signed, None)
        .map_err(|e| e.to_string())?;
    let mut request = reqwest::Client::new().get(url);
    for (name, value) in signed.headers() {
        if name != "host" {
            request = request.header(name.as_str(), value.as_bytes());
        }
    }
    request.send().await.map_err(|e| e.to_string())
}

/// The real endpoint `OSWX_TEST_S3_URL` names, with everything under a prefix of this call's own
/// (so tests, and runs, do not see each other's objects); `None` when it is not set.
pub fn real_s3() -> Option<S3Config> {
    let endpoint = env(S3_ENV)?;
    let path_style = env("OSWX_TEST_S3_PATH_STYLE")
        .is_none_or(|v| !matches!(v.trim(), "0" | "false" | "no" | "off"));
    Some(S3Config {
        bucket: env("OSWX_TEST_S3_BUCKET").unwrap_or_else(|| "oswx-test".into()),
        prefix: format!("t-{}", oswx_server::store::new_id()),
        endpoint: endpoint.trim().trim_end_matches('/').to_owned(),
        region: env("OSWX_TEST_S3_REGION").unwrap_or_else(|| "us-east-1".into()),
        access_key_id: env("OSWX_TEST_S3_ACCESS_KEY").unwrap_or_default(),
        secret_access_key: env("OSWX_TEST_S3_SECRET_KEY").unwrap_or_default(),
        session_token: String::new(),
        path_style,
        tuning: S3Tuning::default(),
    })
}
