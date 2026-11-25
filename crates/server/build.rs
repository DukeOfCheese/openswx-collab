// `sqlx::migrate!` embeds `migrations/` at build time; rebuild when a migration is added or changed.
fn main() {
    println!("cargo:rerun-if-changed=migrations");
}
