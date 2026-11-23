# openswx-collab-server

Self-hostable live collaboration server for [openswx](https://github.com/DukeOfCheese/openswx): several
people editing one CAD model at once, with cursors, feature locks, edit history, named versions and
assemblies that pin part versions. The server never runs a geometry kernel.

```
pip install openswx-collab-server
openswx-collab-server serve
```

PostgreSQL: `pip install "openswx-collab-server[postgres]"`; S3 storage: `[s3]`. A Docker image is at
`ghcr.io/dukeofcheese/openswx-collab-server`.

Documentation, self-hosting guide and the protocol reference: https://github.com/DukeOfCheese/openswx-collab

Apache-2.0. openswx-collab is an independent open-source project, not affiliated with Dassault Systemes.
