//! Who is asking: the account behind a request's bearer token.
//!
//! Handlers that need a signed-in account take [`CurrentUser`]; those that only behave differently
//! for one take [`OptionalUser`]. Both read `Authorization: Bearer <token>` (the scheme in any case,
//! the token trimmed, as the Python server's `bearer()`), and look the session up through
//! [`crate::sessions::Sessions`], which asks the database when it has no recent answer.

use axum::extract::FromRequestParts;
use axum::http::header::AUTHORIZATION;
use axum::http::request::Parts;

use crate::error::ApiError;
use crate::security::token_hash;
use crate::state::AppState;
use crate::store::Id;

/// The signed-in account. Rejects with 401 `sign in first` when the token is missing, malformed or
/// unknown, its session has expired, or its account is disabled or deleted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CurrentUser {
    /// The account's id.
    pub id: Id,
    /// The hash of the token the request presented (which session this is).
    pub token_hash: String,
}

/// The signed-in account, or `None` when the request has no usable session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OptionalUser(pub Option<CurrentUser>);

/// The token in `Authorization: Bearer <token>`, or `""` when there is none.
pub fn bearer(parts: &Parts) -> &str {
    let Some(value) = parts
        .headers
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return "";
    };
    match value.get(..7) {
        Some(scheme) if scheme.eq_ignore_ascii_case("bearer ") => value[7..].trim(),
        _ => "",
    }
}

async fn lookup(parts: &Parts, state: &AppState) -> Result<Option<CurrentUser>, ApiError> {
    let token = bearer(parts);
    if token.is_empty() {
        return Ok(None);
    }
    let hash = token_hash(token);
    let found = state
        .sessions
        .get(state.store.as_ref(), &hash, state.clock.now())
        .await?;
    Ok(found.map(|id| CurrentUser {
        id,
        token_hash: hash,
    }))
}

impl FromRequestParts<AppState> for CurrentUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        lookup(parts, state).await?.ok_or(ApiError::Unauthorized)
    }
}

impl FromRequestParts<AppState> for OptionalUser {
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &AppState) -> Result<Self, ApiError> {
        Ok(Self(lookup(parts, state).await?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    fn parts(auth: Option<&str>) -> Parts {
        let mut b = Request::builder();
        if let Some(a) = auth {
            b = b.header(AUTHORIZATION, a);
        }
        b.body(()).unwrap().into_parts().0
    }

    #[test]
    fn the_bearer_scheme_is_read_in_any_case_and_the_token_trimmed() {
        assert_eq!(bearer(&parts(Some("Bearer abc"))), "abc");
        assert_eq!(bearer(&parts(Some("bEaReR   abc  "))), "abc");
        assert_eq!(bearer(&parts(Some("Basic abc"))), "");
        assert_eq!(bearer(&parts(Some("Bearer"))), "");
        assert_eq!(bearer(&parts(Some("Bearerabc"))), "");
        assert_eq!(bearer(&parts(None)), "");
        let mut latin1 = parts(None);
        latin1.headers.insert(
            AUTHORIZATION,
            axum::http::HeaderValue::from_bytes(b"Bearer \xe9t\xe9").unwrap(),
        );
        assert_eq!(bearer(&latin1), "", "not visible ASCII");
    }
}
