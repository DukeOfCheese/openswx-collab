//! Passwords, tokens, email addresses, rate limits, user colours and role orders.
//!
//! Password hashes are the Python server's: `scrypt$N$r$p$<base64 salt>$<base64 key>`, so accounts
//! imported from an existing installation keep working. Tokens are random and only their SHA-256 is
//! ever stored.

use std::collections::{HashMap, VecDeque};
use std::str::FromStr;
use std::sync::Mutex;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use rand::Rng;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::error::ApiError;

/// scrypt cost of new hashes (the Python server's): N = 2^15, r = 8, p = 1.
const LOG_N: u8 = 15;
const R: u32 = 8;
const P: u32 = 1;
const KEY_LEN: usize = 32;
const SALT_LEN: usize = 16;
/// The Python server refuses to hash with more than 2^27 bytes of memory; so do we, which also keeps a
/// hostile stored hash from asking for gigabytes.
const MAX_SCRYPT_MEMORY: u128 = 1 << 27;

/// The password length limits, in characters: 8 to 200.
pub const PASSWORD_LEN: std::ops::RangeInclusive<usize> = 8..=200;

/// Refuses a password outside [`PASSWORD_LEN`] (counted in characters, as Python counts them).
pub fn check_password_len(pw: &str) -> Result<(), ApiError> {
    let n = pw.chars().count();
    if PASSWORD_LEN.contains(&n) {
        Ok(())
    } else {
        Err(ApiError::BadRequest(
            "password must be 8 to 200 characters".into(),
        ))
    }
}

fn derive(pw: &[u8], salt: &[u8], log_n: u8, r: u32, p: u32) -> Option<[u8; KEY_LEN]> {
    let params = scrypt::Params::new(log_n, r, p, KEY_LEN).ok()?;
    let mut out = [0u8; KEY_LEN];
    scrypt::scrypt(pw, salt, &params, &mut out).ok()?;
    Some(out)
}

/// Hashes a password with a fresh random salt, on the blocking pool (it takes tens of milliseconds
/// and 32 MiB).
pub async fn hash_password(pw: &str) -> String {
    let pw = pw.to_owned();
    tokio::task::spawn_blocking(move || {
        let mut salt = [0u8; SALT_LEN];
        rand::rng().fill(&mut salt[..]);
        let key = derive(pw.as_bytes(), &salt, LOG_N, R, P)
            .expect("the fixed scrypt parameters are valid");
        format!(
            "scrypt${}${R}${P}${}${}",
            1u32 << LOG_N,
            STANDARD.encode(salt),
            STANDARD.encode(key)
        )
    })
    .await
    .expect("password hashing does not panic")
}

/// Whether `pw` is the password behind `stored`, in constant time. A malformed or foreign `stored`
/// (another scheme, bad numbers, bad base64, a key that is not 32 bytes, parameters needing more
/// than 128 MiB) is `false`.
pub async fn verify_password(pw: &str, stored: &str) -> bool {
    let (pw, stored) = (pw.to_owned(), stored.to_owned());
    tokio::task::spawn_blocking(move || verify_blocking(pw.as_bytes(), &stored))
        .await
        .unwrap_or(false)
}

fn verify_blocking(pw: &[u8], stored: &str) -> bool {
    let parts: Vec<&str> = stored.split('$').collect();
    let [scheme, n, r, p, salt, key] = parts[..] else {
        return false;
    };
    if scheme != "scrypt" {
        return false;
    }
    let (Ok(n), Ok(r), Ok(p)) = (n.parse::<u64>(), r.parse::<u32>(), p.parse::<u32>()) else {
        return false;
    };
    if n < 2 || !n.is_power_of_two() {
        return false;
    }
    if 128 * u128::from(r) * (u128::from(n) + u128::from(p)) > MAX_SCRYPT_MEMORY {
        return false;
    }
    let (Ok(salt), Ok(want)) = (STANDARD.decode(salt), STANDARD.decode(key)) else {
        return false;
    };
    if want.len() != KEY_LEN {
        return false;
    }
    let Some(got) = derive(pw, &salt, n.trailing_zeros() as u8, r, p) else {
        return false;
    };
    got.ct_eq(&want[..]).into()
}

/// A new random token: 32 random bytes as 43 URL-safe base64 characters (Python's
/// `secrets.token_urlsafe(32)`).
pub fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::rng().fill(&mut bytes[..]);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// What is stored for a token: its SHA-256 as 64 lower-case hex characters.
pub fn token_hash(token: &str) -> String {
    hex::encode(Sha256::digest(token.as_bytes()))
}

/// Python's `\s` for `str` patterns: Unicode white space plus the four separators U+001C..U+001F.
fn is_space(c: char) -> bool {
    c.is_whitespace() || ('\u{1c}'..='\u{1f}').contains(&c)
}

/// Trims and lower-cases an address and checks its shape: `local@domain.tld` with one `@`, no
/// white space, a local part of 1 to 64 characters, a domain part of 1 to 255 characters before the
/// last dot and a last label of 2 or more characters. A bad address is `BadRequest("not an email
/// address")`.
pub fn clean_email(s: &str) -> Result<String, ApiError> {
    let e = s.trim_matches(is_space).to_lowercase();
    if is_email(&e) {
        Ok(e)
    } else {
        Err(ApiError::BadRequest("not an email address".into()))
    }
}

fn is_email(e: &str) -> bool {
    let plain = |c: char| c != '@' && !is_space(c);
    let Some((local, rest)) = e.split_once('@') else {
        return false;
    };
    let Some((host, tld)) = rest.rsplit_once('.') else {
        return false;
    };
    (1..=64).contains(&local.chars().count())
        && local.chars().all(plain)
        && (1..=255).contains(&host.chars().count())
        && host.chars().all(plain)
        && tld.chars().count() >= 2
        && tld.chars().all(plain)
}

/// The most keys a [`RateLimit`] remembers; past it the least recently used are forgotten.
const MAX_KEYS: usize = 100_000;

/// An in-memory limit on how often something may happen per key, over a sliding window (as the Python
/// server's `Limiter`): a key is blocked while it has `max` or more recorded hits in the last
/// `window` seconds.
///
/// There are two ways to use it. [`RateLimit::check`] counts the call and says whether it is allowed
/// (password reset: every request counts). [`RateLimit::blocked`], [`RateLimit::fail`] and
/// [`RateLimit::reset`] count only what the caller says (sign-in: only failures count, success
/// clears). Times are seconds, from the caller's clock.
#[derive(Debug)]
pub struct RateLimit {
    max: usize,
    window: f64,
    hits: Mutex<HashMap<String, VecDeque<f64>>>,
}

impl RateLimit {
    /// Blocks a key at `max` hits within `window_secs`.
    pub fn new(max: usize, window_secs: u64) -> Self {
        Self {
            max,
            window: window_secs as f64,
            hits: Mutex::default(),
        }
    }

    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<String, VecDeque<f64>>> {
        self.hits.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn prune(&self, q: &mut VecDeque<f64>, now: f64) {
        while q.front().is_some_and(|t| *t < now - self.window) {
            q.pop_front();
        }
    }

    /// Whether `key` is over its limit now. Records nothing.
    pub fn blocked(&self, key: &str, now: f64) -> bool {
        let mut map = self.map();
        let Some(q) = map.get_mut(key) else {
            return false;
        };
        self.prune(q, now);
        if q.is_empty() {
            map.remove(key);
            return false;
        }
        q.len() >= self.max
    }

    /// Records a hit for `key` at `now`.
    pub fn fail(&self, key: &str, now: f64) {
        let mut map = self.map();
        if !map.contains_key(key) && map.len() >= MAX_KEYS {
            self.make_room(&mut map, now);
        }
        let q = map.entry(key.to_owned()).or_default();
        self.prune(q, now);
        // A blocked key gains nothing from more hits; this bounds a queue at `max` entries.
        if q.len() < self.max.max(1) {
            q.push_back(now);
        }
    }

    /// Counts a call for `key`: false (and nothing recorded) when it is already blocked, otherwise
    /// true with the call recorded.
    pub fn check(&self, key: &str, now: f64) -> bool {
        if self.blocked(key, now) {
            return false;
        }
        self.fail(key, now);
        true
    }

    /// Forgets `key`.
    pub fn reset(&self, key: &str) {
        self.map().remove(key);
    }

    /// Frees space for a new key: first the expired ones, then, if that was not enough, the least
    /// recently hit tenth of the table.
    fn make_room(&self, map: &mut HashMap<String, VecDeque<f64>>, now: f64) {
        map.retain(|_, q| q.back().is_some_and(|t| *t >= now - self.window));
        if map.len() < MAX_KEYS {
            return;
        }
        let mut by_age: Vec<(f64, String)> = map
            .iter()
            .map(|(k, q)| (q.back().copied().unwrap_or(0.0), k.clone()))
            .collect();
        by_age.sort_by(|a, b| a.0.total_cmp(&b.0));
        for (_, key) in by_age.into_iter().take(MAX_KEYS / 10) {
            map.remove(&key);
        }
    }

    /// How many keys are remembered (for tests).
    pub fn len(&self) -> usize {
        self.map().len()
    }

    /// Whether no key is remembered.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

const PALETTE: [&str; 12] = [
    "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4", "#008080", "#f032e6", "#9a6324",
    "#800000", "#469990", "#000075", "#808000",
];

/// A stable colour per user, moved along the palette while someone else in the document has it
/// (`taken`); the user's own first choice when all twelve are taken.
pub fn colour_for(user_id: &str, taken: &[String]) -> String {
    // The digest as one big number, modulo 12, a hex digit at a time.
    let start = hex::encode(Sha256::digest(user_id.as_bytes()))
        .bytes()
        .fold(0usize, |acc, d| {
            (acc * 16 + (d as char).to_digit(16).unwrap_or(0) as usize) % PALETTE.len()
        });
    (0..PALETTE.len())
        .map(|n| PALETTE[(start + n) % PALETTE.len()])
        .find(|c| !taken.iter().any(|t| t == c))
        .unwrap_or(PALETTE[start])
        .to_owned()
}

/// A role in an organisation, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum OrgRole {
    Member,
    Admin,
    Owner,
}

impl OrgRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Member => "member",
            Self::Admin => "admin",
            Self::Owner => "owner",
        }
    }
}

impl FromStr for OrgRole {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "member" => Ok(Self::Member),
            "admin" => Ok(Self::Admin),
            "owner" => Ok(Self::Owner),
            _ => Err(()),
        }
    }
}

/// A role in a project, lowest first.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum ProjectRole {
    Viewer,
    Editor,
    Owner,
}

impl ProjectRole {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Editor => "editor",
            Self::Owner => "owner",
        }
    }
}

impl FromStr for ProjectRole {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, ()> {
        match s {
            "viewer" => Ok(Self::Viewer),
            "editor" => Ok(Self::Editor),
            "owner" => Ok(Self::Owner),
            _ => Err(()),
        }
    }
}

/// Whether role `have` is `need` or higher, both read in the organisation order
/// (`owner > admin > member`) or both in the project order (`owner > editor > viewer`). Roles from
/// different orders, and unknown roles, are never enough.
pub fn role_at_least(have: &str, need: &str) -> bool {
    if let (Ok(h), Ok(n)) = (have.parse::<OrgRole>(), need.parse::<OrgRole>()) {
        return h >= n;
    }
    if let (Ok(h), Ok(n)) = (have.parse::<ProjectRole>(), need.parse::<ProjectRole>()) {
        return h >= n;
    }
    false
}
