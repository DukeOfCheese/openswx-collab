# Contributing to openswx-collab

Thank you for helping. This repository is the collaboration server and wire protocol for
[openswx](https://github.com/DukeOfCheese/openswx); it follows the same rules as openswx, and this file
only adds what is specific to a network service.

- [The clean-room rule](#the-clean-room-rule)
- [What is open and what is not](#what-is-open-and-what-is-not)
- [Reporting: issues](#reporting-issues)
- [Sending a change: pull requests](#sending-a-change-pull-requests)
- [Development](#development)
- [Security](#security)
- [Licence](#licence)

## The clean-room rule

openswx contains none of SOLIDWORKS' code or artwork, and neither does this repository. The rule in
[openswx's CONTRIBUTING.md](https://github.com/DukeOfCheese/openswx/blob/main/CONTRIBUTING.md#the-clean-room-rule)
applies here unchanged: no decompiled or copied SOLIDWORKS code, no SOLIDWORKS artwork, no Dassault sample
files, and no files saved by SOLIDWORKS in the repository, in issues or in pull requests. A pull request
that breaks it is closed, whatever else it contains.

The same goes for other collaboration products: describe behaviour you want in your own words. Do not paste
another product's code, protocol captures or documentation.

## What is open and what is not

Everything in this repository is Apache-2.0. What a hosted service adds on top (seat limits today; single
sign-on and billing later) lives in a separate, private crate that depends on the server library and
plugs in through the `Extensions` trait (`crates/server/src/ext.rs`), building its own binary with
`oswx_serverd::run`. The open server must stay complete and useful by itself: a change that moves existing
behaviour behind an extension point will not be merged.

## Reporting: issues

Open an issue with one of the forms at
[New issue](https://github.com/DukeOfCheese/openswx-collab/issues/new/choose). One problem per issue.

What makes a server report quick to act on:

1. **What you ran**: the server version (`GET /api/server`), how it is deployed (the binary, Docker
   Compose; the Rust server 0.2 or the Python server 0.1), the database (SQLite or PostgreSQL) and where
   files are stored (folder or S3).
2. **A reproduction as requests.** The REST calls and WebSocket messages that show the problem, as text
   (`docs/protocol.md` describes them; WebSocket frames are MessagePack, so show them decoded). For a
   sync problem, the `change` messages each client sent, in order, and what each received.
3. **Expected vs actual**, with the response or message you got.
4. **Server log lines** around the time, as text. Remove tokens, email addresses and document content you
   do not want public before posting.

Labels follow openswx's scheme (type, area, priority P0 to P3, status); the list is in
[`.github/labels.yml`](.github/labels.yml).

Problems with openswx itself (modelling, files, the desktop app) belong in the
[openswx tracker](https://github.com/DukeOfCheese/openswx/issues). Security problems never go in a public
issue: see [SECURITY.md](SECURITY.md).

## Sending a change: pull requests

- **One topic per pull request**, small enough to review in one sitting.
- Link the issue (`Fixes #123`). For anything bigger than a bug fix, agree the approach in the issue first.
- **Tests come with the change.** For anything a client can send, include the hostile version too: the
  malformed message, the wrong role, the other organisation's document.
- **A change to the protocol** (`crates/core/src/wire.rs`, `op.rs`, REST routes, close codes) updates
  `docs/protocol.md` in the same pull request (a test fails when a message, field or code is missing from
  it) and says whether older clients keep working. Bump `PROTOCOL_VERSION` when they do not.
- **A change to the tables** comes with a new migration in `crates/server/migrations/sqlite/` and
  `crates/server/migrations/postgres/` (AGENTS.md says what else to watch). Released migrations are never
  edited.
- **A new setting** goes in `crates/server/src/config.rs`, the `--help` text of `crates/serverd` (a test
  holds the two together) and `docs/self-hosting.md`.
- **The server never runs a geometry kernel** and never depends on openswx. Anything that needs geometry
  belongs in the openswx client.
- CI must pass: `cargo fmt`, `clippy` and the tests on Linux, Windows and macOS, the server's suite on
  PostgreSQL and with files on Garage (S3), and the Docker image built and run. The Python server's suite
  runs too while that line is kept.
- Fill in the pull request template; the checklist is the review checklist.

Commit messages: a short summary line in the imperative or as a noun phrase, a blank line, then what changed
and why. Pull requests are squash-merged, so the PR title becomes the commit summary.

## Development

The server is Rust (stable, from [rustup](https://rustup.rs)):

```bash
cargo run -p oswx-serverd -- serve                       # http://127.0.0.1:8787, SQLite and files in ./data
cargo fmt --check
cargo clippy --workspace --all-targets -j 2 -- -D warnings
cargo test --workspace -j 2                              # everything on SQLite (S3 tests use an in-process server)
python tools/test_postgres_rust.py                       # the server's suite on a throwaway PostgreSQL
                                                         #   (pip install pixeltable-pgserver first)
python tools/check_deploy_rust.py                        # the Rust Dockerfile and Compose files (pip install pyyaml)
python tools/check_deploy_rust.py --docker               # ... built and run, where Docker is available
```

`-j 2` keeps a full test build within 16 GB of memory. The Python server (the 0.1 line) still has its own
suite: `pip install -e protocol -e "server[test]"`, then `python -m pytest -q`, `python
tools/test_postgres.py` and `python tools/check_deploy.py`.

How the code is organised and the rules for coding agents: [AGENTS.md](AGENTS.md).

## Releasing

A tag `v<version>` releases (`.github/workflows/release.yml`). A `v0.1.*` tag releases the Python line (0.1.x):
its packages, PyPI, its image (which keeps `latest`) and the GitHub release. Any other `v*` tag releases the
Rust server: binaries for Linux x86_64 and aarch64 (static), Windows x86_64 and macOS arm64 and x86_64, each
an archive with a `.sha256`; the image `ghcr.io/dukeofcheese/openswx-collab-server` with the tags
`<version>` and `<major>.<minor>` only (never `latest`); and the GitHub release. For the Rust server:

1. Bump `version` in both `crates/server/Cargo.toml` and `crates/serverd/Cargo.toml` (they must agree,
   and match the tag), and let `Cargo.lock` follow (`cargo build`).
2. In `CHANGELOG.md`, move the version's notes out of `## Unreleased` into a section of their own headed
   `## <version> - <date>`, as the earlier ones are (`## 0.1.2 - 2026-11-24`; for 0.2.0: the
   `### 0.2.0 (not yet released)` heading becomes `## 0.2.0 - <date>`, and its subheadings move up a
   level). `python tools/release_notes.py <version>` prints the release notes and fails if the section
   is missing.
3. Before tagging, run the release workflow by hand on the version-bump branch
   (`gh workflow run release.yml --ref <branch>`) and check that every build job passes.
4. Merge that to `main`, then tag it there: `git tag -a v<version> -m "openswx-collab <version>"` and
   `git push origin v<version>`.

A run by hand from the Actions tab (`release`, on any branch) builds both lines without publishing
anything, so a broken build shows before the tag.

## Security

See [SECURITY.md](SECURITY.md). Please report vulnerabilities privately, never in a public issue.

## Conduct

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Licence

Contributions are accepted under the Apache License 2.0 (see [LICENSE](LICENSE)). By opening a pull request
you confirm that you wrote the change, or have the right to submit it under that licence, and that it follows
the clean-room rule.
