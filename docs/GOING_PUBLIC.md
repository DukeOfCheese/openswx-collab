# Going public: checklist

**Switched to public on 2026-11-23.** What was done, in order, and what is still open. It mirrors
[openswx's checklist](https://github.com/DukeOfCheese/openswx/blob/main/docs/GOING_PUBLIC.md). The files a
public project needs are already in the tree (README, CONTRIBUTING, AGENTS, SECURITY, CODE_OF_CONDUCT,
NOTICE, CHANGELOG, issue forms, pull request template, CODEOWNERS, Dependabot, labels, CI).

## Before switching

- [x] **History scan (2026-11-23).** No SOLIDWORKS files, databases or `.env` files ever committed. No local
      user names, home paths, private addresses or credential patterns anywhere in history. Every commit uses
      the GitHub no-reply address.
- [x] Run the scan again just before switching:
      ```
      git log --all --name-only --format= | sort -u | grep -iE "\.(sldprt|sldasm|slddrw|db|sqlite|env)$"
      git log --all -p | grep -nE "ghp_|github_pat_|sk-ant-|AKIA[0-9A-Z]{16}|PRIVATE KEY|C:\\\\Users"
      ```
      Both should print nothing.
- [x] **The commercial layer is out of this repository** (2026-11-23): it lives in the private
      `openswx-collab-ee`.
- [x] **History squashed** (2026-11-23): `main` is one commit with no trace of the commercial layer.
- [x] **No old history left on GitHub** (2026-11-23): the repository was deleted and created again with the
      squashed `main`, which removed the pull-request references (`refs/pull/*`) that still held the old
      history. Check again just before switching with `git ls-remote origin`: any `refs/pull/*` listed must
      belong to pull requests opened since.
- [ ] **The name.** The same question as openswx's ("SW"): this repository follows whatever openswx decides,
      and the package names (`openswx-collab-protocol`, `openswx-collab-server`) and the `OSWX_COLLAB_`
      settings prefix would change with it. Cheapest before the first release.
- [x] Told the openswx side it is public, so its README can link here.
- [ ] Tag a first release once the openswx client can talk to it, with a `CHANGELOG.md` section for the
      version (openswx's `tools/release_notes.py` turns that into the release description).

## Repository settings (GitHub → Settings)

- [x] Description and topics (set 2026-11-23). Description: "Self-hostable live collaboration for openswx:
      several people editing one CAD model at once, with cursors, feature locks, edit history, named
      versions and assemblies that pin part versions." Topics: `cad`, `parametric-cad`,
      `mechanical-engineering`, `collaboration`, `real-time-collaboration`, `self-hosted`, `websocket`,
      `fastapi`, `python`, `openswx`. Like openswx's, they leave SOLIDWORKS out.
- [x] **General**: turn on *Discussions*; turn off *Wiki*. Merge button: *squash merging* only, with the PR
      title as the commit message, and *automatically delete head branches*.
- [ ] **Branch protection** for `main` (Settings → Rules → Rulesets): require a pull request, require the
      `test` workflow's checks (`test`, `postgres`, `docker`), block force pushes and deletion.
- [x] **Security**: turn on *Private vulnerability reporting* (SECURITY.md and the issue chooser link to it;
      it can only be switched on once the repository is public), *Dependabot alerts* and *security updates*,
      *secret scanning* with *push protection*, and *CodeQL* default setup (Python).
- [x] **Actions**: require approval for first-time contributors' workflows; workflow permissions read-only
      (the workflow already asks for `contents: read` only).
- [x] **Labels**: `python ../openswx/tools/sync_labels.py`, run from this repository (creates the labels in
      `.github/labels.yml`).

## Worth doing soon after

- [x] **Releases** (`.github/workflows/release.yml`): on a `v*` tag, the Docker image for amd64 and arm64
      to GHCR, the Python packages (to PyPI once trusted publishing is set up), and the GitHub release.
- [ ] **PyPI**: `release.yml` builds and checks both packages on every tag; publishing waits for the two
      pending publishers on pypi.org (environments `pypi` and `pypi-server`) and `PYPI_PUBLISH=true` (the
      steps are at the top of `release.yml`).
      The openswx client depends on `openswx-collab-protocol` being there.
- [x] **Database migrations** (Alembic; the server upgrades on start, `openswx-collab-server migrate`).
- [ ] Cross-link the two READMEs.
- [ ] Optional: a public `DukeOfCheese/.github` repository holding the Code of Conduct and security policy
      for every repository of the account, so the two projects cannot drift apart.
