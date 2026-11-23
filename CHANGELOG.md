# Changelog

User-visible changes, newest first. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and versions follow [Semantic Versioning](https://semver.org/) (before 1.0, a minor version may change the
protocol or the database; the change is listed here).

## Unreleased

## 0.1.0 - 2026-11-23

The first release: the server and the protocol. The openswx side (the in-app toggle, sync and live
cursors) is not released yet; it is being built on openswx's `dev/openswx-collab` branch.

### Added
- The wire protocol (`openswx-collab-protocol`): id-keyed operations on features, components and mates,
  `diff` / `apply`, and the WebSocket messages. Protocol version 1.
- Accounts: email and password, sign-in with Google or GitHub for desktop apps, sessions, invites.
  The first account on a server is its admin; everyone else joins by invite unless registration is opened.
- Organisations with owner / admin / member roles, projects with owner / editor / viewer roles.
- Cloud documents: turn a part or an assembly into a shared document; large imported bodies travel as
  content-addressed blobs.
- Live editing: the server orders changes, refuses conflicting ones and says who got there first; feature
  locks with a grace period across short disconnects; server-issued feature ids and names; catch-up after a
  reconnect; cursors, selections, active tools and in-progress previews relayed to everyone in the file.
- History: every change with its author; per-user undo and revert of any change; rebuild failures pinned on
  the change that caused them; named versions; restore to any point.
- Assemblies: components use parts by reference, pinned to a point of the part's history; where-used;
  a notice when a part gets a newer named version.
- Storage on SQLite or PostgreSQL, with files in a folder or an S3-compatible bucket; Dockerfile and
  Compose files.
- Extension hooks (`hooks.py`) for what a hosted service adds on top, such as seat limits.
- Database migrations (Alembic): the server upgrades its database on start; `openswx-collab-server migrate`
  and `db-status` for operators who turn that off.
- Deleting your account (your work stays with the organisation) and exporting your data; deleting an
  organisation with everything in it, and exporting it as a zip.
- Optional email over SMTP: invites made for an address are emailed, and people can reset a forgotten
  password themselves.
