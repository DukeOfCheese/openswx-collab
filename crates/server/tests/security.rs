//! Passwords, tokens, email addresses, rate limits, colours and roles.

use oswx_server::ApiError;
use oswx_server::security::*;

/// Made by `server/openswx_collab_server/security.py` `hash_password` (the Python server).
const PYTHON_HASH: &str =
    "scrypt$32768$8$1$qrFBU8J48jhxrwvkhABmhA==$sQk9nUEocascWx3x3f2pksVAErjMqJEX13dyQtuIlfs=";
const PYTHON_PASSWORD: &str = "correct horse battery staple \u{fc}";

#[tokio::test]
async fn a_hash_made_by_the_python_server_verifies() {
    assert!(verify_password(PYTHON_PASSWORD, PYTHON_HASH).await);
}

#[tokio::test]
async fn a_wrong_password_fails() {
    assert!(!verify_password("correct horse battery staple u", PYTHON_HASH).await);
    assert!(!verify_password("", PYTHON_HASH).await);
}

#[tokio::test]
async fn a_new_hash_has_the_python_format_and_verifies() {
    let h = hash_password("hunter2hunter2").await;
    let parts: Vec<&str> = h.split('$').collect();
    assert_eq!(&parts[..4], ["scrypt", "32768", "8", "1"], "{h}");
    assert_eq!(parts.len(), 6);
    assert_eq!(parts[4].len(), 24); // 16 bytes, padded base64
    assert_eq!(parts[5].len(), 44); // 32 bytes, padded base64
    assert!(verify_password("hunter2hunter2", &h).await);
    assert!(!verify_password("hunter2hunter3", &h).await);
    assert_ne!(h, hash_password("hunter2hunter2").await, "salts differ");
}

#[tokio::test]
async fn a_malformed_stored_hash_is_false() {
    let good_key = "sQk9nUEocascWx3x3f2pksVAErjMqJEX13dyQtuIlfs=";
    let salt = "qrFBU8J48jhxrwvkhABmhA==";
    for stored in [
        "".to_owned(),
        "scrypt".into(),
        "bcrypt$32768$8$1$x$y".into(),
        format!("scrypt$32768$8$1${salt}"),
        format!("scrypt$32768$8$1${salt}${good_key}$extra"),
        format!("scrypt$x$8$1${salt}${good_key}"),
        format!("scrypt$32768$-8$1${salt}${good_key}"),
        format!("scrypt$32767$8$1${salt}${good_key}"), // not a power of two
        format!("scrypt$1$8$1${salt}${good_key}"),
        format!("scrypt$0$8$1${salt}${good_key}"),
        format!("scrypt$32768$8$1$!!!${good_key}"),
        format!("scrypt$32768$8$1${salt}$!!!"),
        format!("scrypt$32768$8$1${salt}$AAAA"), // key too short
        format!("scrypt$32768$0$1${salt}${good_key}"),
        format!("scrypt$32768$8$0${salt}${good_key}"),
        // would need far more than 128 MiB: refused, not attempted
        format!("scrypt$1073741824$8$1${salt}${good_key}"),
        format!("scrypt$32768$1024$1${salt}${good_key}"),
        "scrypt$99999999999999999999$8$1$x$y".into(),
    ] {
        assert!(!verify_password(PYTHON_PASSWORD, &stored).await, "{stored}");
    }
}

#[test]
fn password_length_is_8_to_200_characters() {
    for ok in ["12345678", &"a".repeat(200), &"\u{fc}".repeat(200)] {
        assert!(check_password_len(ok).is_ok(), "{}", ok.len());
    }
    for bad in ["", "1234567", &"a".repeat(201), &"\u{fc}".repeat(7)] {
        assert!(
            matches!(check_password_len(bad), Err(ApiError::BadRequest(_))),
            "{}",
            bad.len()
        );
    }
}

#[test]
fn tokens_are_43_url_safe_characters_and_differ() {
    let a = new_token();
    assert_eq!(a.len(), 43);
    assert!(
        a.bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    );
    assert_ne!(a, new_token());
}

#[test]
fn token_hash_is_lower_case_hex_sha256() {
    assert_eq!(
        token_hash("abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );
    assert_eq!(
        token_hash(""),
        "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
    );
}

#[test]
fn email_is_trimmed_lower_cased_and_checked() {
    assert_eq!(
        clean_email("  Ann@Example.COM \n").unwrap(),
        "ann@example.com"
    );
    assert_eq!(clean_email("a@b.cd").unwrap(), "a@b.cd");
    assert_eq!(clean_email("a@b.c.de").unwrap(), "a@b.c.de");
    assert_eq!(clean_email("a@b..cd").unwrap(), "a@b..cd"); // the Python pattern allows it
    let long_local = format!("{}@x.io", "a".repeat(64));
    assert!(clean_email(&long_local).is_ok());
    for bad in [
        "",
        "   ",
        "plain",
        "@example.com",
        "a@",
        "a@b",
        "a@b.c",
        "a@.com",
        "a@b.c.",
        "a@@b.com",
        "a@b@c.com",
        "a b@example.com",
        "a@exa mple.com",
        "a@example.c m",
        "a@example.co\tm",
    ] {
        match clean_email(bad) {
            Err(ApiError::BadRequest(m)) => assert_eq!(m, "not an email address"),
            other => panic!("{bad:?} gave {other:?}"),
        }
    }
    assert!(clean_email(&format!("{}@x.io", "a".repeat(65))).is_err());
    assert!(clean_email(&format!("a@{}.io", "b".repeat(255))).is_ok());
    assert!(clean_email(&format!("a@{}.io", "b".repeat(256))).is_err());
}

#[test]
fn the_login_limit_is_ten_failures_in_ten_minutes_and_success_clears_it() {
    let limit = RateLimit::new(10, 600);
    let key = "ann@example.com|10.0.0.1";
    for i in 0..9 {
        assert!(!limit.blocked(key, f64::from(i)));
        limit.fail(key, f64::from(i));
    }
    assert!(!limit.blocked(key, 9.0));
    limit.fail(key, 9.0);
    assert!(limit.blocked(key, 10.0), "the tenth failure blocks");
    assert!(!limit.blocked("ann@example.com|10.0.0.2", 10.0), "per key");
    // Failures leave the window one by one: at 600 the one from 0 is exactly at the edge and stays.
    assert!(limit.blocked(key, 600.0));
    assert!(!limit.blocked(key, 600.5), "the failure from t=0 has left");
    // Success clears.
    for i in 0..10 {
        limit.fail(key, 1000.0 + f64::from(i));
    }
    assert!(limit.blocked(key, 1010.0));
    limit.reset(key);
    assert!(!limit.blocked(key, 1010.0));
}

#[test]
fn the_reset_limit_is_five_requests_an_hour() {
    let limit = RateLimit::new(5, 3600);
    for i in 0..5 {
        assert!(limit.check("ann@example.com", f64::from(i)), "{i}");
    }
    assert!(!limit.check("ann@example.com", 5.0));
    assert!(!limit.check("ann@example.com", 3000.0));
    assert!(limit.check("bob@example.com", 5.0));
    // The first request, at t=0, leaves the window after an hour; one slot opens.
    assert!(limit.check("ann@example.com", 3600.5));
    assert!(!limit.check("ann@example.com", 3601.0));
}

#[test]
fn the_limiter_forgets_expired_keys_and_caps_its_memory() {
    let limit = RateLimit::new(3, 60);
    limit.fail("a", 0.0);
    assert!(!limit.blocked("a", 100.0));
    assert!(
        limit.is_empty(),
        "an expired key is dropped when it is looked at"
    );

    for i in 0..100_000 {
        limit.fail(&format!("k{i}"), 1000.0 + f64::from(i) * 1e-4);
    }
    assert_eq!(limit.len(), 100_000);
    // A new key at the cap evicts the oldest tenth when nothing has expired.
    limit.fail("new", 1011.0);
    assert!(limit.len() <= 90_001, "{}", limit.len());
    assert!(!limit.blocked("k0", 1011.0), "the oldest was evicted");
    // Expired keys go first: refill to the cap, then add one long after the window.
    for i in 0..(100_000 - limit.len()) {
        limit.fail(&format!("m{i}"), 1012.0);
    }
    assert_eq!(limit.len(), 100_000);
    limit.fail("later", 5000.0);
    assert_eq!(limit.len(), 1);
}

#[test]
fn colours_match_the_python_palette() {
    let none: [String; 0] = [];
    for (id, want) in [
        ("00000000000000000000000000000000", "#f032e6"),
        ("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "#f58231"),
        ("0123456789abcdef0123456789abcdef", "#008080"),
        ("ffffffffffffffffffffffffffffffff", "#469990"),
    ] {
        assert_eq!(colour_for(id, &none), want, "{id}");
    }
}

#[test]
fn a_taken_colour_moves_along_the_palette_and_wraps() {
    let id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"; // starts at #f58231
    let taken = vec!["#f58231".to_owned()];
    assert_eq!(colour_for(id, &taken), "#911eb4"); // as Python
    let all: Vec<String> = [
        "#e6194b", "#3cb44b", "#4363d8", "#f58231", "#911eb4", "#008080", "#f032e6", "#9a6324",
        "#800000", "#469990", "#000075", "#808000",
    ]
    .map(String::from)
    .to_vec();
    assert_eq!(colour_for(id, &all), "#f58231", "all taken: the user's own");
    // Only the one just before the start is free: the walk wraps all the way round to it.
    let mut most = all.clone();
    most.retain(|c| c != "#4363d8");
    assert_eq!(colour_for(id, &most), "#4363d8");
}

#[test]
fn role_orders() {
    assert!(role_at_least("owner", "owner"));
    assert!(role_at_least("owner", "admin"));
    assert!(role_at_least("admin", "member"));
    assert!(!role_at_least("member", "admin"));
    assert!(!role_at_least("admin", "owner"));
    assert!(role_at_least("owner", "editor"));
    assert!(role_at_least("editor", "viewer"));
    assert!(role_at_least("editor", "editor"));
    assert!(!role_at_least("viewer", "editor"));
    assert!(!role_at_least("editor", "owner"));
    // Different orders and unknown names are never enough.
    assert!(!role_at_least("admin", "viewer"));
    assert!(!role_at_least("editor", "member"));
    assert!(!role_at_least("root", "member"));
    assert!(!role_at_least("owner", "root"));
    assert!(!role_at_least("", ""));
}

#[test]
fn roles_parse_and_print() {
    for r in [OrgRole::Member, OrgRole::Admin, OrgRole::Owner] {
        assert_eq!(r.as_str().parse::<OrgRole>(), Ok(r));
    }
    for r in [ProjectRole::Viewer, ProjectRole::Editor, ProjectRole::Owner] {
        assert_eq!(r.as_str().parse::<ProjectRole>(), Ok(r));
    }
    assert!("Owner".parse::<OrgRole>().is_err());
    assert!("editor".parse::<OrgRole>().is_err());
    assert!(OrgRole::Owner > OrgRole::Admin && OrgRole::Admin > OrgRole::Member);
    assert!(ProjectRole::Owner > ProjectRole::Editor && ProjectRole::Editor > ProjectRole::Viewer);
}
