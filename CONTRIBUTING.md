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
sign-on and billing later) lives in a separate, private package that plugs in through
`server/openswx_collab_server/hooks.py`. The open server must stay complete and useful by itself: a change
that moves existing behaviour behind a hook will not be merged.

## Reporting: issues

Open an issue with one of the forms at
[New issue](https://github.com/DukeOfCheese/openswx-collab/issues/new/choose). One problem per issue.

What makes a server report quick to act on:

1. **What you ran**: the server version (`GET /api/server`), how it is deployed (plain Python, Docker
   Compose), the database (SQLite or PostgreSQL) and where files are stored (folder or S3).
2. **A reproduction as requests.** The REST calls and WebSocket messages that show the problem, as text
   (`docs/protocol.md` describes them). For a sync problem, the `change` messages each client sent, in
   order, and what each received.
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
- **A change to the protocol** (`protocol/`, message or operation shapes, REST routes, close codes) updates
  `docs/protocol.md` in the same pull request and says whether older clients keep working. Bump
  `PROTOCOL_VERSION` when they do not.
- **A change to the tables** (`server/openswx_collab_server/db.py`) comes with an Alembic revision in
  `server/openswx_collab_server/migrations/versions/` (AGENTS.md says how to make one).
- **The server never runs a geometry kernel** and never depends on openswx. Anything that needs geometry
  belongs in the openswx client.
- CI must pass: the suite on SQLite and PostgreSQL, and the Docker image build.
- Fill in the pull request template; the checklist is the review checklist.

Commit messages: a short summary line in the imperative or as a noun phrase, a blank line, then what changed
and why. Pull requests are squash-merged, so the PR title becomes the commit summary.

## Development

```bash
python -m venv .venv
.venv/bin/pip install -e protocol -e "server[test]"      # Windows: .venv\Scripts\pip
python -m pytest -q                                      # the whole suite on SQLite, about a minute
python tools/test_postgres.py                            # the same on a throwaway PostgreSQL
python tools/check_deploy.py                             # the Dockerfile and Compose settings, without Docker
python -m openswx_collab_server serve                    # http://127.0.0.1:8787
python tools/e2e_two_peers.py ../openswx                 # two real openswx processes; needs an openswx checkout
```

How the code is organised and the rules for coding agents: [AGENTS.md](AGENTS.md).

## Security

See [SECURITY.md](SECURITY.md). Please report vulnerabilities privately, never in a public issue.

## Conduct

Everyone taking part follows the [Code of Conduct](CODE_OF_CONDUCT.md).

## Licence

Contributions are accepted under the Apache License 2.0 (see [LICENSE](LICENSE)). By opening a pull request
you confirm that you wrote the change, or have the right to submit it under that licence, and that it follows
the clean-room rule.
