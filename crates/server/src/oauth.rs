//! Google and GitHub as this server talks to them: trading the code a provider gave the browser for
//! an access token, and asking who that token belongs to.
//!
//! The calls go through [`OAuthHttp`] ([`crate::AppState::oauth`]) so that tests can answer for the
//! providers. A running server uses [`HttpProviders`]: the providers' fixed HTTPS addresses (the
//! constants below, the ones the Python server calls), rustls, [`CALL_TIMEOUT`] per request, response
//! bodies of at most [`MAX_BODY`], no redirects followed. The routes in `routes/oauth.rs` also bound
//! every call by [`OAuthHttp::timeout`], whatever the implementation does.
//!
//! Nothing here logs: a provider's code, an access token and an email address never reach the log.

use std::fmt;
use std::time::Duration;

use async_trait::async_trait;
use serde_json::Value;

use crate::config::Settings;

/// How long one request to a provider may take.
pub const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// The largest response body read from a provider: 1 MiB.
pub const MAX_BODY: usize = 1 << 20;

/// The longest access token accepted from a provider (real ones are a few hundred characters).
const MAX_ACCESS_TOKEN: usize = 4096;

/// The longest subject kept (the `oauth_identities.subject` column).
pub const MAX_SUBJECT: usize = 255;

/// A provider's addresses and the scope this server asks for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Endpoints {
    /// Where the browser is sent to sign in.
    pub authorize: &'static str,
    /// Where the code is traded for an access token.
    pub token: &'static str,
    /// Who the token belongs to.
    pub userinfo: &'static str,
    /// The account's email addresses (GitHub only).
    pub emails: Option<&'static str>,
    pub scope: &'static str,
}

/// Google's addresses.
pub const GOOGLE: Endpoints = Endpoints {
    authorize: "https://accounts.google.com/o/oauth2/v2/auth",
    token: "https://oauth2.googleapis.com/token",
    userinfo: "https://openidconnect.googleapis.com/v1/userinfo",
    emails: None,
    scope: "openid email profile",
};

/// GitHub's addresses.
pub const GITHUB: Endpoints = Endpoints {
    authorize: "https://github.com/login/oauth/authorize",
    token: "https://github.com/login/oauth/access_token",
    userinfo: "https://api.github.com/user",
    emails: Some("https://api.github.com/user/emails"),
    scope: "read:user user:email",
};

/// A provider this server can sign people in through.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Provider {
    Google,
    Github,
}

impl Provider {
    /// The provider called `name` (`google` or `github`), if there is one.
    pub fn parse(name: &str) -> Option<Self> {
        match name {
            "google" => Some(Self::Google),
            "github" => Some(Self::Github),
            _ => None,
        }
    }

    /// Its name, as the routes, the settings and `oauth_identities.provider` spell it.
    pub fn name(self) -> &'static str {
        match self {
            Self::Google => "google",
            Self::Github => "github",
        }
    }

    /// Its addresses.
    pub fn endpoints(self) -> &'static Endpoints {
        match self {
            Self::Google => &GOOGLE,
            Self::Github => &GITHUB,
        }
    }
}

/// An access token from a provider. Its `Debug` does not show it.
#[derive(Clone, PartialEq, Eq)]
pub struct ProviderToken(String);

impl ProviderToken {
    pub fn new(token: impl Into<String>) -> Self {
        Self(token.into())
    }

    /// The token itself, for the `Authorization` header.
    pub fn secret(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for ProviderToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ProviderToken(..)")
    }
}

/// Who a provider says signed in.
#[derive(Clone, PartialEq, Eq)]
pub struct Identity {
    /// The provider's id for the person (Google's `sub`, GitHub's `id`), 1 to [`MAX_SUBJECT`]
    /// characters.
    pub subject: String,
    /// An email address the provider gave, as it gave it.
    pub email: Option<String>,
    /// Whether the provider says the person owns `email`. An unverified address is never used to
    /// find or create an account.
    pub email_verified: bool,
    /// A display name (may be empty).
    pub name: String,
}

impl fmt::Debug for Identity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Identity")
            .field("subject", &self.subject)
            .field("email", &self.email.as_ref().map(|_| ".."))
            .field("email_verified", &self.email_verified)
            .finish_non_exhaustive()
    }
}

/// Why a provider call did not give an answer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum OAuthError {
    /// The provider answered, and said no (the code is not valid, say): `provider_refused`.
    #[error("the provider refused")]
    Refused,
    /// The provider could not be reached, did not answer in time, or answered something this server
    /// does not understand: `provider_unreachable`.
    #[error("the provider could not be reached or answered badly")]
    Unreachable,
}

/// The two calls a sign-in makes to a provider.
#[async_trait]
pub trait OAuthHttp: Send + Sync + 'static {
    /// How long the routes wait for one call ([`CALL_TIMEOUT`] by default); a call that takes longer
    /// is dropped and counts as [`OAuthError::Unreachable`].
    fn timeout(&self) -> Duration {
        CALL_TIMEOUT
    }

    /// Trades the `code` the provider gave the browser for an access token. `redirect_to_server`
    /// is this server's callback address, which the provider checks against the one the sign-in
    /// started with.
    async fn exchange_code(
        &self,
        provider: Provider,
        code: &str,
        redirect_to_server: &str,
    ) -> Result<ProviderToken, OAuthError>;

    /// Who `token` belongs to.
    async fn fetch_identity(
        &self,
        provider: Provider,
        token: &ProviderToken,
    ) -> Result<Identity, OAuthError>;
}

/// No providers: every call is [`OAuthError::Unreachable`]. What [`crate::AppState::new`] starts
/// with; a running server has [`HttpProviders`].
#[derive(Debug, Default, Clone, Copy)]
pub struct NoProviders;

#[async_trait]
impl OAuthHttp for NoProviders {
    async fn exchange_code(
        &self,
        _: Provider,
        _: &str,
        _: &str,
    ) -> Result<ProviderToken, OAuthError> {
        Err(OAuthError::Unreachable)
    }

    async fn fetch_identity(&self, _: Provider, _: &ProviderToken) -> Result<Identity, OAuthError> {
        Err(OAuthError::Unreachable)
    }
}

/// A client id and secret.
#[derive(Clone)]
struct Credentials {
    id: String,
    secret: String,
}

/// The real providers, over HTTPS (see the module documentation).
pub struct HttpProviders {
    client: reqwest::Client,
    google: Credentials,
    github: Credentials,
}

impl fmt::Debug for HttpProviders {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("HttpProviders").finish_non_exhaustive()
    }
}

impl HttpProviders {
    /// The providers with the client ids and secrets of `settings`.
    pub fn new(settings: &Settings) -> anyhow::Result<Self> {
        let client = reqwest::Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(CALL_TIMEOUT)
            .connect_timeout(CALL_TIMEOUT)
            // GitHub's API refuses requests without one.
            .user_agent(concat!("openswx-collab-server/", env!("CARGO_PKG_VERSION")))
            .build()?;
        Ok(Self {
            client,
            google: Credentials {
                id: settings.google_client_id.clone(),
                secret: settings.google_client_secret.clone(),
            },
            github: Credentials {
                id: settings.github_client_id.clone(),
                secret: settings.github_client_secret.clone(),
            },
        })
    }

    fn credentials(&self, provider: Provider) -> &Credentials {
        match provider {
            Provider::Google => &self.google,
            Provider::Github => &self.github,
        }
    }

    /// GETs `url` with the token and answers the JSON body.
    async fn get_json(&self, url: &str, token: &ProviderToken) -> Result<Value, OAuthError> {
        let response = self
            .client
            .get(url)
            .bearer_auth(token.secret())
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            .map_err(|_| OAuthError::Unreachable)?;
        if !response.status().is_success() {
            return Err(OAuthError::Unreachable);
        }
        json(&read_capped(response, MAX_BODY).await?)
    }
}

#[async_trait]
impl OAuthHttp for HttpProviders {
    async fn exchange_code(
        &self,
        provider: Provider,
        code: &str,
        redirect_to_server: &str,
    ) -> Result<ProviderToken, OAuthError> {
        let credentials = self.credentials(provider);
        let form = [
            ("client_id", credentials.id.as_str()),
            ("client_secret", credentials.secret.as_str()),
            ("code", code),
            ("redirect_uri", redirect_to_server),
            ("grant_type", "authorization_code"),
        ];
        let response = self
            .client
            .post(provider.endpoints().token)
            .header(reqwest::header::ACCEPT, "application/json")
            .form(&form)
            .send()
            .await
            .map_err(|_| OAuthError::Unreachable)?;
        let status = response.status();
        // A refused code: GitHub answers 200 with an `error`, Google 400 `invalid_grant`.
        if status == reqwest::StatusCode::BAD_REQUEST || status == reqwest::StatusCode::UNAUTHORIZED
        {
            return Err(OAuthError::Refused);
        }
        if !status.is_success() {
            return Err(OAuthError::Unreachable);
        }
        token_from(&json(&read_capped(response, MAX_BODY).await?)?)
    }

    async fn fetch_identity(
        &self,
        provider: Provider,
        token: &ProviderToken,
    ) -> Result<Identity, OAuthError> {
        let endpoints = provider.endpoints();
        let info = self.get_json(endpoints.userinfo, token).await?;
        match (provider, endpoints.emails) {
            (Provider::Github, Some(emails)) => {
                let emails = self.get_json(emails, token).await?;
                github_identity(&info, &emails)
            }
            _ => google_identity(&info),
        }
    }
}

/// Reads at most `max` bytes of a response's body: [`OAuthError::Unreachable`] if it is longer.
pub async fn read_capped(
    mut response: reqwest::Response,
    max: usize,
) -> Result<Vec<u8>, OAuthError> {
    if response.content_length().is_some_and(|n| n > max as u64) {
        return Err(OAuthError::Unreachable);
    }
    let mut body = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| OAuthError::Unreachable)?
    {
        if body.len() + chunk.len() > max {
            return Err(OAuthError::Unreachable);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

fn json(body: &[u8]) -> Result<Value, OAuthError> {
    serde_json::from_slice(body).map_err(|_| OAuthError::Unreachable)
}

/// The access token in a token endpoint's answer: [`OAuthError::Refused`] when it has none (GitHub
/// answers a bad code with `{"error": ...}`), [`OAuthError::Unreachable`] when the answer is not a
/// JSON object or the token is not something that can go in a header.
pub fn token_from(answer: &Value) -> Result<ProviderToken, OAuthError> {
    let Some(answer) = answer.as_object() else {
        return Err(OAuthError::Unreachable);
    };
    match answer.get("access_token").and_then(Value::as_str) {
        None | Some("") => Err(OAuthError::Refused),
        Some(token)
            if token.len() <= MAX_ACCESS_TOKEN && token.bytes().all(|b| b.is_ascii_graphic()) =>
        {
            Ok(ProviderToken::new(token))
        }
        Some(_) => Err(OAuthError::Unreachable),
    }
}

/// A subject: a string or a whole number (GitHub's ids are numbers), 1 to [`MAX_SUBJECT`]
/// characters.
fn subject(value: Option<&Value>) -> Result<String, OAuthError> {
    let subject = match value {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Number(n)) if n.is_u64() || n.is_i64() => n.to_string(),
        _ => return Err(OAuthError::Unreachable),
    };
    if subject.is_empty() || subject.contains('\0') || subject.chars().count() > MAX_SUBJECT {
        return Err(OAuthError::Unreachable);
    }
    Ok(subject)
}

fn text(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(str::to_owned)
}

/// Google's userinfo answer: `sub`, `email` (verified when `email_verified` is `true` or `"true"`),
/// `name`.
pub fn google_identity(info: &Value) -> Result<Identity, OAuthError> {
    if !info.is_object() {
        return Err(OAuthError::Unreachable);
    }
    let verified = matches!(info.get("email_verified"), Some(Value::Bool(true)))
        || info.get("email_verified").and_then(Value::as_str) == Some("true");
    Ok(Identity {
        subject: subject(info.get("sub"))?,
        email: text(info.get("email")),
        email_verified: verified,
        name: text(info.get("name")).unwrap_or_default(),
    })
}

/// GitHub's `/user` and `/user/emails` answers: `id`; the email of the entry that is both `primary`
/// and `verified` (none otherwise: GitHub's public `email` field is not used); `name`, or `login`
/// when there is none.
pub fn github_identity(user: &Value, emails: &Value) -> Result<Identity, OAuthError> {
    if !user.is_object() {
        return Err(OAuthError::Unreachable);
    }
    let Some(emails) = emails.as_array() else {
        return Err(OAuthError::Unreachable);
    };
    let primary = emails.iter().find_map(|e| {
        let yes = |key| e.get(key) == Some(&Value::Bool(true));
        (yes("primary") && yes("verified"))
            .then(|| text(e.get("email")))
            .flatten()
    });
    let name = text(user.get("name"))
        .filter(|n| !n.is_empty())
        .or_else(|| text(user.get("login")))
        .unwrap_or_default();
    Ok(Identity {
        subject: subject(user.get("id"))?,
        email_verified: primary.is_some(),
        email: primary,
        name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_token_answer_without_a_token_is_a_refusal() {
        assert_eq!(
            token_from(&json!({"access_token": "gho_abc"})).unwrap(),
            ProviderToken::new("gho_abc")
        );
        for refused in [
            json!({"error": "bad_verification_code"}),
            json!({"access_token": ""}),
            json!({"access_token": null}),
            json!({"access_token": 5}),
        ] {
            assert_eq!(token_from(&refused), Err(OAuthError::Refused), "{refused}");
        }
        for broken in [
            json!([]),
            json!("x"),
            json!({"access_token": "two words"}),
            json!({"access_token": "line\nbreak"}),
            json!({"access_token": "x".repeat(MAX_ACCESS_TOKEN + 1)}),
        ] {
            assert_eq!(token_from(&broken), Err(OAuthError::Unreachable));
        }
        assert!(!format!("{:?}", ProviderToken::new("gho_abc")).contains("gho_abc"));
    }

    #[test]
    fn google_vouches_for_an_address_only_when_it_says_so() {
        let id = google_identity(&json!({"sub": "123", "email": "a@example.com",
            "email_verified": true, "name": "Ada"}))
        .unwrap();
        assert_eq!(
            (
                id.subject.as_str(),
                id.email.as_deref(),
                id.email_verified,
                id.name.as_str()
            ),
            ("123", Some("a@example.com"), true, "Ada")
        );
        assert!(
            google_identity(&json!({"sub": "1", "email_verified": "true"}))
                .unwrap()
                .email_verified
        );
        for unverified in [
            json!(false),
            json!("false"),
            json!("yes"),
            json!(1),
            Value::Null,
        ] {
            let id = google_identity(&json!({"sub": "1", "email": "a@example.com",
                "email_verified": unverified}))
            .unwrap();
            assert!(!id.email_verified);
        }
        for bad in [
            json!({}),
            json!({"sub": ""}),
            json!({"sub": {"x": 1}}),
            json!({"sub": 1.5}),
            json!({"sub": "x".repeat(MAX_SUBJECT + 1)}),
            json!([]),
        ] {
            assert_eq!(google_identity(&bad), Err(OAuthError::Unreachable), "{bad}");
        }
        let id =
            google_identity(&json!({"sub": "1", "email": "a@example.com", "email_verified": true}))
                .unwrap();
        assert!(!format!("{id:?}").contains("a@example.com"));
    }

    #[test]
    fn github_gives_the_primary_verified_address_only() {
        let user = json!({"id": 42, "login": "ada", "name": null});
        let emails = json!([
            {"email": "public@example.com", "primary": false, "verified": true},
            {"email": "ada@example.com", "primary": true, "verified": true},
        ]);
        let id = github_identity(&user, &emails).unwrap();
        assert_eq!(id.subject, "42");
        assert_eq!(id.email.as_deref(), Some("ada@example.com"));
        assert!(id.email_verified);
        assert_eq!(id.name, "ada");

        let unverified = json!([
            {"email": "ada@example.com", "primary": true, "verified": false},
            {"email": "other@example.com", "primary": false, "verified": true},
            {"email": "truthy@example.com", "primary": "yes", "verified": "yes"},
        ]);
        let id = github_identity(
            &json!({"id": 42, "login": "ada", "name": "Ada L"}),
            &unverified,
        )
        .unwrap();
        assert_eq!((id.email, id.email_verified), (None, false));
        assert_eq!(id.name, "Ada L");

        assert_eq!(
            github_identity(&user, &json!({})),
            Err(OAuthError::Unreachable)
        );
        assert_eq!(
            github_identity(&json!({"login": "x"}), &emails),
            Err(OAuthError::Unreachable)
        );
    }

    async fn serve(router: axum::Router) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
        base
    }

    #[tokio::test]
    async fn a_body_over_the_cap_is_not_read() {
        use axum::routing::get;
        let big = vec![b'a'; MAX_BODY + 1];
        let exact = vec![b'a'; MAX_BODY];
        let streamed = big.clone();
        let app = axum::Router::new()
            .route("/big", get(move || async move { big.clone() }))
            .route("/exact", get(move || async move { exact.clone() }))
            .route(
                "/streamed",
                get(move || async move {
                    let chunks = streamed
                        .chunks(64 * 1024)
                        .map(|c| Ok::<_, std::io::Error>(c.to_vec()))
                        .collect::<Vec<_>>();
                    axum::body::Body::from_stream(futures_util::stream::iter(chunks))
                }),
            );
        let base = serve(app).await;
        let http = reqwest::Client::new();
        let get = |path: &str| http.get(format!("{base}{path}")).send();
        assert_eq!(
            read_capped(get("/exact").await.unwrap(), MAX_BODY)
                .await
                .unwrap()
                .len(),
            MAX_BODY
        );
        assert_eq!(
            read_capped(get("/big").await.unwrap(), MAX_BODY).await,
            Err(OAuthError::Unreachable)
        );
        let response = get("/streamed").await.unwrap();
        assert!(response.content_length().is_none());
        assert_eq!(
            read_capped(response, MAX_BODY).await,
            Err(OAuthError::Unreachable)
        );
    }

    #[tokio::test]
    async fn the_real_client_goes_only_to_https() {
        let providers = HttpProviders::new(&Settings::default()).unwrap();
        // http:// is refused before any connection (https_only).
        let err = providers
            .client
            .get("http://127.0.0.1:9/")
            .send()
            .await
            .unwrap_err();
        assert!(
            err.is_builder() || err.to_string().contains("https"),
            "{err}"
        );
        for endpoints in [GOOGLE, GITHUB] {
            for url in [endpoints.authorize, endpoints.token, endpoints.userinfo]
                .into_iter()
                .chain(endpoints.emails)
            {
                assert!(url.starts_with("https://"), "{url}");
            }
        }
        assert!(!format!("{providers:?}").contains("secret"));
    }
}
