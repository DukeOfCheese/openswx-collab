//! Errors as clients see them: a status code and FastAPI's body shape, `{"detail": "<message>"}`,
//! which the openswx client already parses.

use axum::Json;
use axum::extract::{FromRequest, FromRequestParts, Path, Query, Request};
use axum::http::StatusCode;
use axum::http::request::Parts;
use axum::response::{IntoResponse, Response};
use serde::Serialize;
use serde::de::DeserializeOwned;

/// What a handler answers when it cannot do what was asked. Every handler returns
/// `Result<_, ApiError>`.
///
/// Any error that converts into `anyhow::Error` converts into `ApiError::Internal` with `?`: the
/// client is told only "server error", and the error is logged.
#[derive(Debug)]
pub enum ApiError {
    /// 404 with the message (e.g. `no such project`; `Not Found` for an unknown route).
    NotFound(&'static str),
    /// 404 with a message made for the request (e.g. `the document has no state 9`).
    NotFoundDetail(String),
    /// 401 `sign in first`.
    Unauthorized,
    /// 401 `wrong email address or password` (a refused sign-in).
    WrongCredentials,
    /// 403 with the message.
    Forbidden(&'static str),
    /// 409 with the message.
    Conflict(String),
    /// 409 whose `detail` is this JSON object rather than a string (as some Python 409s are, e.g.
    /// `{"message": "upload these blobs first", "blobs": [...]}`). Its [`ApiError::detail`] is the
    /// object's `message`.
    ConflictWith(serde_json::Value),
    /// 400 with the message.
    BadRequest(String),
    /// 422 with the message: well-formed input that fails validation (as FastAPI answers).
    Unprocessable(String),
    /// 402 with the message (an extension refused, e.g. a seat limit).
    PaymentRequired(String),
    /// 413 `too large`.
    TooLarge,
    /// 429 `too many attempts; try again in a few minutes`.
    RateLimited,
    /// 429 with the message: the server is doing as much of this as it does at once.
    Busy(&'static str),
    /// 501 with the message.
    NotImplemented(String),
    /// 503 with the message: the server could not get to the request in time; nothing was done.
    Unavailable(&'static str),
    /// 500 `server error`; the error itself goes to the log only.
    Internal(anyhow::Error),
}

impl ApiError {
    /// The status code this error answers with.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) | Self::NotFoundDetail(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized | Self::WrongCredentials => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) | Self::ConflictWith(_) => StatusCode::CONFLICT,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::Unprocessable(_) => StatusCode::UNPROCESSABLE_ENTITY,
            Self::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RateLimited | Self::Busy(_) => StatusCode::TOO_MANY_REQUESTS,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Unavailable(_) => StatusCode::SERVICE_UNAVAILABLE,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The `detail` the client sees.
    pub fn detail(&self) -> &str {
        match self {
            Self::Unauthorized => "sign in first",
            Self::WrongCredentials => "wrong email address or password",
            Self::NotFound(m) | Self::Forbidden(m) | Self::Busy(m) | Self::Unavailable(m) => m,
            Self::Conflict(m)
            | Self::NotFoundDetail(m)
            | Self::BadRequest(m)
            | Self::Unprocessable(m)
            | Self::PaymentRequired(m)
            | Self::NotImplemented(m) => m,
            Self::ConflictWith(v) => v
                .get("message")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("conflict"),
            Self::TooLarge => "too large",
            Self::RateLimited => "too many attempts; try again in a few minutes",
            Self::Internal(_) => "server error",
        }
    }
}

/// What a request is told whose text holds the zero character (U+0000), which PostgreSQL cannot
/// store: refused on both databases, so that they hold the same.
pub const ZERO_CHARACTER: &str = "text must not contain the zero character";

/// What a request is told when the database ended its transaction in favour of another one.
pub const TRY_AGAIN: &str = "another request changed the same thing at the same moment; try again";

/// What a request is told when the database could not get to it in time.
pub const BUSY: &str = "the server is busy; try again";

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    /// 500, except for what the database says about itself rather than about a fault:
    ///
    /// - two transactions got in each other's way, or one waited too long for the other's lock
    ///   ([`crate::store::is_conflict`]): nothing was written and the request can be sent again,
    ///   409 [`TRY_AGAIN`];
    /// - no connection came free in time, or a statement ran out of time
    ///   ([`crate::store::is_busy`]): 503 [`BUSY`];
    /// - PostgreSQL refused the zero character in a value no route checked for it (an id in a
    ///   path, say), which is the request's fault: 422 [`ZERO_CHARACTER`].
    fn from(err: E) -> Self {
        let err = err.into();
        if crate::store::is_zero_character(&err) {
            return Self::Unprocessable(ZERO_CHARACTER.into());
        }
        if crate::store::is_conflict(&err) {
            tracing::warn!("a request lost to a concurrent one: {err:#}");
            return Self::Conflict(TRY_AGAIN.into());
        }
        if crate::store::is_busy(&err) {
            tracing::warn!("the database did not get to a request in time: {err:#}");
            return Self::Unavailable(BUSY);
        }
        Self::Internal(err)
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        match &self {
            Self::Internal(err) => tracing::error!("request failed: {err:#}"),
            Self::ConflictWith(detail) => {
                return (
                    StatusCode::CONFLICT,
                    Json(serde_json::json!({ "detail": detail })),
                )
                    .into_response();
            }
            _ => {}
        }
        detail_response(self.status(), self.detail())
    }
}

/// A response with the given status and body `{"detail": detail}`.
pub fn detail_response(status: StatusCode, detail: &str) -> Response {
    (status, Json(serde_json::json!({ "detail": detail }))).into_response()
}

/// A JSON request body, and a JSON response. Use this rather than `axum::Json` for every body: a body
/// that is not JSON (400), is not the expected shape (422), is not declared as JSON (415) or is over
/// the route's size limit (413) is refused with a `{"detail": "..."}` body rather than plain text.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApiJson<T>(pub T);

impl<T, S> FromRequest<S> for ApiJson<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<T>::from_request(req, state).await {
            Ok(Json(value)) => Ok(Self(value)),
            Err(rejection) if rejection.status() == StatusCode::PAYLOAD_TOO_LARGE => {
                Err(ApiError::TooLarge.into_response())
            }
            Err(rejection) => Err(detail_response(rejection.status(), &rejection.body_text())),
        }
    }
}

impl<T: Serialize> IntoResponse for ApiJson<T> {
    fn into_response(self) -> Response {
        Json(self.0).into_response()
    }
}

/// Path parameters. Use this rather than `axum::extract::Path`: a segment that does not parse is 400
/// with a `{"detail": "..."}` body rather than plain text.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApiPath<T>(pub T);

impl<T, S> FromRequestParts<S> for ApiPath<T>
where
    T: DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = Response;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Response> {
        match Path::<T>::from_request_parts(parts, state).await {
            Ok(Path(value)) => Ok(Self(value)),
            // A segment that does not parse is the client's (400); a route without the parameter
            // the handler asks for is our bug (500, logged, not explained).
            Err(rejection) if rejection.status().is_server_error() => {
                Err(ApiError::Internal(anyhow::anyhow!(rejection.body_text())).into_response())
            }
            Err(rejection) => Err(detail_response(rejection.status(), &rejection.body_text())),
        }
    }
}

/// Query parameters. Use this rather than `axum::extract::Query`: a query string that does not parse
/// is 400 with a `{"detail": "..."}` body rather than plain text.
#[derive(Debug, Clone, Copy, Default)]
pub struct ApiQuery<T>(pub T);

impl<T, S> FromRequestParts<S> for ApiQuery<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, ApiError> {
        match Query::<T>::from_request_parts(parts, state).await {
            Ok(Query(value)) => Ok(Self(value)),
            Err(rejection) => Err(ApiError::BadRequest(rejection.body_text())),
        }
    }
}
