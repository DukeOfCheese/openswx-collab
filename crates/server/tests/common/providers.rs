//! A stand-in for Google and GitHub, as the Python tests' `fake_provider`: the code the browser
//! brings back is the person's key ("ada"), the access token is `at-<key>`, and the identity is
//! built from the JSON each provider would answer, parsed by the server's own parsers.

use std::collections::HashMap;
use std::sync::{Mutex, PoisonError};
use std::time::Duration;

use async_trait::async_trait;
use oswx_server::oauth::{
    Identity, OAuthError, OAuthHttp, Provider, ProviderToken, github_identity, google_identity,
};
use serde_json::json;

/// Someone who can sign in at the fake providers.
#[derive(Debug, Clone)]
pub struct Person {
    /// Who the provider says this is (Google's `sub` is `g-<subject>`, GitHub's `id` `h-<subject>`).
    pub subject: String,
    pub email: String,
    /// Whether the providers vouch for `email` (GitHub: whether it is the primary verified entry).
    pub verified: bool,
}

#[derive(Debug, Default)]
struct Inner {
    /// By the code the browser brings back.
    people: HashMap<String, Person>,
    /// Every call fails as unreachable.
    unreachable: bool,
    /// Every call waits forever.
    hang: bool,
    /// Calls made, of either kind.
    calls: usize,
    /// The redirect addresses the code exchanges were told.
    redirects: Vec<String>,
}

/// The fake providers; [`super::TestServer`] puts one in every server it starts.
#[derive(Debug)]
pub struct FakeProviders {
    inner: Mutex<Inner>,
    timeout: Mutex<Duration>,
}

impl Default for FakeProviders {
    fn default() -> Self {
        Self {
            inner: Mutex::default(),
            timeout: Mutex::new(oswx_server::oauth::CALL_TIMEOUT),
        }
    }
}

impl FakeProviders {
    fn inner(&self) -> std::sync::MutexGuard<'_, Inner> {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// `code` signs in as someone with the verified address `email` (as the Python fake's `emails`).
    pub fn person(&self, code: &str, email: &str) {
        self.person_with(code, email, true);
    }

    /// `code` signs in as someone with `email`, vouched for or not.
    pub fn person_with(&self, code: &str, email: &str, verified: bool) {
        self.answer(code, code, email, verified);
    }

    /// From now on `code` signs in as the provider account `subject` with `email` (a provider that
    /// answers the same code with someone else).
    pub fn answer(&self, code: &str, subject: &str, email: &str, verified: bool) {
        self.inner().people.insert(
            code.to_owned(),
            Person {
                subject: subject.to_owned(),
                email: email.to_owned(),
                verified,
            },
        );
    }

    /// Every call fails as unreachable (or no longer does).
    pub fn unreachable(&self, yes: bool) {
        self.inner().unreachable = yes;
    }

    /// Every call waits forever (or no longer does).
    pub fn hang(&self, yes: bool) {
        self.inner().hang = yes;
    }

    /// How long the server waits for one call.
    pub fn set_timeout(&self, timeout: Duration) {
        *self.timeout.lock().unwrap_or_else(PoisonError::into_inner) = timeout;
    }

    /// How many calls were made.
    pub fn calls(&self) -> usize {
        self.inner().calls
    }

    /// The callback addresses the code exchanges sent.
    pub fn redirects(&self) -> Vec<String> {
        self.inner().redirects.clone()
    }

    /// Counts a call; `Err` when it should fail as unreachable, `Ok(true)` when it should hang.
    fn call(&self) -> Result<bool, OAuthError> {
        let mut inner = self.inner();
        inner.calls += 1;
        if inner.unreachable {
            return Err(OAuthError::Unreachable);
        }
        Ok(inner.hang)
    }
}

#[async_trait]
impl OAuthHttp for FakeProviders {
    fn timeout(&self) -> Duration {
        *self.timeout.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn exchange_code(
        &self,
        _provider: Provider,
        code: &str,
        redirect_to_server: &str,
    ) -> Result<ProviderToken, OAuthError> {
        if self.call()? {
            std::future::pending::<()>().await;
        }
        let mut inner = self.inner();
        inner.redirects.push(redirect_to_server.to_owned());
        if inner.people.contains_key(code) {
            Ok(ProviderToken::new(format!("at-{code}")))
        } else {
            Err(OAuthError::Refused)
        }
    }

    async fn fetch_identity(
        &self,
        provider: Provider,
        token: &ProviderToken,
    ) -> Result<Identity, OAuthError> {
        if self.call()? {
            std::future::pending::<()>().await;
        }
        let who = token
            .secret()
            .strip_prefix("at-")
            .ok_or(OAuthError::Refused)?;
        let person = self
            .inner()
            .people
            .get(who)
            .cloned()
            .ok_or(OAuthError::Refused)?;
        match provider {
            Provider::Google => google_identity(&json!({
                "sub": format!("g-{}", person.subject),
                "email": person.email,
                "email_verified": person.verified,
                "name": who,
            })),
            Provider::Github => github_identity(
                &json!({"id": format!("h-{}", person.subject), "login": who, "name": null}),
                &json!([
                    {"email": "other@example.com", "primary": false, "verified": true},
                    {"email": person.email, "primary": true, "verified": person.verified},
                ]),
            ),
        }
    }
}
