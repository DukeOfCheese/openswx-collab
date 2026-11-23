<!-- One topic per pull request. The checklist below is the review checklist. -->

## What and why

<!-- What changes, and why. Link the issue: "Fixes #123". -->

## How it was checked

<!-- The tests added or changed. For anything a client can send, the hostile case as well as the friendly one. -->

## Checklist

- [ ] Follows the clean-room rule: no SOLIDWORKS code, artwork or files, and nothing copied from another collaboration product
- [ ] The open server stays complete: nothing that works today is moved behind an extension hook
- [ ] Tests added or updated; `python -m pytest -q` passes locally
- [ ] Changes to `hub.py` or `ops.py`: `tests/test_convergence.py` passes, and `targets()` names everything `apply()` touches
- [ ] Protocol changes (messages, operations, REST routes, close codes): `docs/protocol.md` updated, and it says whether older clients keep working
- [ ] Table changes (`db.py`): how an existing database gets there is described
- [ ] No token, password, invite or document content is logged
- [ ] Third-party code: licence compatible with Apache-2.0 and listed in `NOTICE`
- [ ] User-visible change noted in `CHANGELOG.md` under *Unreleased*
