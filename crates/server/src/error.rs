//! Errors as clients see them: a status code and FastAPI's body shape, `{"detail": "<message>"}`,
//! which the openswx client already parses.

use axum::Json;
use axum::extract::{FromRequest, Request};
use axum::http::StatusCode;
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
    /// 401 `sign in first`.
    Unauthorized,
    /// 403 with the message.
    Forbidden(&'static str),
    /// 409 with the message.
    Conflict(String),
    /// 400 with the message.
    BadRequest(String),
    /// 402 with the message (an extension refused, e.g. a seat limit).
    PaymentRequired(String),
    /// 413 `too large`.
    TooLarge,
    /// 429 `too many attempts; try again in a few minutes`.
    RateLimited,
    /// 501 with the message.
    NotImplemented(String),
    /// 500 `server error`; the error itself goes to the log only.
    Internal(anyhow::Error),
}

impl ApiError {
    /// The status code this error answers with.
    pub fn status(&self) -> StatusCode {
        match self {
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
            Self::Forbidden(_) => StatusCode::FORBIDDEN,
            Self::Conflict(_) => StatusCode::CONFLICT,
            Self::BadRequest(_) => StatusCode::BAD_REQUEST,
            Self::PaymentRequired(_) => StatusCode::PAYMENT_REQUIRED,
            Self::TooLarge => StatusCode::PAYLOAD_TOO_LARGE,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::NotImplemented(_) => StatusCode::NOT_IMPLEMENTED,
            Self::Internal(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }

    /// The `detail` the client sees.
    pub fn detail(&self) -> &str {
        match self {
            Self::Unauthorized => "sign in first",
            Self::NotFound(m) | Self::Forbidden(m) => m,
            Self::Conflict(m)
            | Self::BadRequest(m)
            | Self::PaymentRequired(m)
            | Self::NotImplemented(m) => m,
            Self::TooLarge => "too large",
            Self::RateLimited => "too many attempts; try again in a few minutes",
            Self::Internal(_) => "server error",
        }
    }
}

impl<E: Into<anyhow::Error>> From<E> for ApiError {
    fn from(err: E) -> Self {
        Self::Internal(err.into())
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        if let Self::Internal(err) = &self {
            tracing::error!("request failed: {err:#}");
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
