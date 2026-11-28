//! The page a password reset link opens, `GET /reset-password/{token}`: a form that sets the new
//! password through `POST /api/auth/password-reset/confirm`, under a content security policy that
//! allows nothing but its own script and style.

use std::sync::LazyLock;

use axum::http::header::{
    CACHE_CONTROL, CONTENT_SECURITY_POLICY, CONTENT_TYPE, REFERRER_POLICY, X_CONTENT_TYPE_OPTIONS,
};
use axum::http::{HeaderMap, HeaderValue};
use axum::response::{IntoResponse, Response};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use sha2::{Digest, Sha256};

use crate::error::ApiError;

const RESET_STYLE: &str = "body{font:16px system-ui;max-width:28em;margin:4em auto;padding:0 1em}\
input{width:100%;font:inherit;padding:.4em}button{font:inherit;padding:.4em 1em}";

/// Reads the token from the page's own address (it is never written into the page), checks the two
/// fields match, and posts them.
const RESET_SCRIPT: &str = r#"
const token = decodeURIComponent(location.pathname.split("/").pop() || "");
document.getElementById("f").onsubmit = async (e) => {
  e.preventDefault();
  const m = document.getElementById("m"), p = document.getElementById("p").value;
  if (p !== document.getElementById("q").value) { m.textContent = "The two passwords differ."; return; }
  const r = await fetch("/api/auth/password-reset/confirm", {method: "POST",
    headers: {"content-type": "application/json"}, body: JSON.stringify({token, password: p})});
  const d = await r.json().catch(() => ({}));
  m.textContent = r.ok ? d.message : (typeof d.detail === "string" ? d.detail : "That did not work.");
  if (r.ok) document.getElementById("f").remove();
};
"#;

/// The page, the same for every link.
static RESET_PAGE: LazyLock<String> = LazyLock::new(|| {
    format!(
        r#"<!doctype html><meta charset=utf-8><meta name=viewport content="width=device-width">
<meta name=referrer content=no-referrer><title>Choose a new password</title>
<style>{RESET_STYLE}</style>
<body>
<h1>Choose a new password</h1>
<form id=f><p><label>New password<br><input id=p type=password minlength=8 maxlength=200 required autocomplete=new-password></label></p>
<p><label>Again<br><input id=q type=password minlength=8 maxlength=200 required autocomplete=new-password></label></p>
<p><button>Set password</button></p></form><p id=m role=status></p>
<script>{RESET_SCRIPT}</script></body>"#
    )
});

fn csp_hash(source: &str) -> String {
    format!(
        "'sha256-{}'",
        STANDARD.encode(Sha256::digest(source.as_bytes()))
    )
}

/// Nothing but the page's own script and style, requests to this server only, no framing, and no
/// form submission (if the script does not run, the password never goes into a URL).
static RESET_CSP: LazyLock<String> = LazyLock::new(|| {
    format!(
        "default-src 'none'; script-src {}; style-src {}; connect-src 'self'; base-uri 'none'; \
         form-action 'none'; frame-ancestors 'none'",
        csp_hash(RESET_SCRIPT),
        csp_hash(RESET_STYLE)
    )
});

/// The page a reset link opens. The token in the address is not read here: the page reads it.
pub(crate) async fn reset_page() -> Response {
    let mut headers = HeaderMap::new();
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert(REFERRER_POLICY, HeaderValue::from_static("no-referrer"));
    headers.insert(X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff"));
    match HeaderValue::from_str(&RESET_CSP) {
        Ok(csp) => {
            headers.insert(CONTENT_SECURITY_POLICY, csp);
        }
        Err(err) => return ApiError::Internal(err.into()).into_response(),
    }
    (headers, RESET_PAGE.as_str()).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reset_page_policy_allows_exactly_its_own_script_and_style() {
        let page = RESET_PAGE.as_str();
        assert!(page.contains(&format!("<script>{RESET_SCRIPT}</script>")));
        assert!(page.contains(&format!("<style>{RESET_STYLE}</style>")));
        assert!(!page.contains("style=\""), "no inline style attributes");
        assert!(RESET_CSP.contains(&csp_hash(RESET_SCRIPT)));
        assert!(RESET_CSP.starts_with("default-src 'none';"));
    }
}
