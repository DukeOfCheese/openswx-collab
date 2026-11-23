# Security policy

## Reporting a vulnerability

Please report it **privately**, through GitHub's
[private vulnerability reporting](https://github.com/DukeOfCheese/openswx-collab/security/advisories/new)
(*Security → Report a vulnerability*). Do not open a public issue, discussion or pull request for it.

Include what an attacker can do, the steps or a proof of concept, the version, and how the server is deployed
(database, file storage, reverse proxy). You will get an acknowledgement within 7 days and a plan within 30.
Fixes are released before the details are published, and you are credited in the advisory unless you would
rather not be.

A problem in the openswx application itself goes to
[openswx's security page](https://github.com/DukeOfCheese/openswx/security/advisories/new).

## Supported versions

openswx-collab is in alpha: only the latest release (and `main`) gets security fixes.

## What is in scope

This is a network service that holds people's designs and accounts, so these are the areas we care most
about:

- **Accounts and sessions** (`auth.py`, `security.py`): signing in as someone else, keeping access after a
  password change or sign-out, taking over an account through Google/GitHub sign-in or an invite, and
  anything that turns the browser sign-in flow into a way to redirect a code to another machine; using a
  password reset link that was not sent to you, or finding out who has an account.
- **Access between people** (`access.py`, `orgs.py`, `documents.py`, `assemblies.py`): reading or changing a
  document, blob, version or history in a project or organisation you are not in; raising your own role;
  learning the name or content of something you cannot open.
- **The live protocol** (`hub.py`, `protocol/`): a message that corrupts a document, makes the server's copy
  differ from its stored history, takes over another person's connection or locks, or crashes or hangs the
  server for everyone.
- **Stored files** (`storage.py`): reading or writing outside the storage folder or bucket prefix.
- **Extension hooks** (`hooks.py`): getting past a limit an installed extension sets, such as seats.
- **Through the server to other people's apps**: a presence, draft, name or message that makes another
  person's openswx run a command, touch a file, show injected markup or lock up. The server relays these
  unchecked, so such a report usually concerns the openswx client and belongs on
  [openswx's security page](https://github.com/DukeOfCheese/openswx/security/advisories/new) as well; report
  it to either and we will move it.

Out of scope: anything that needs an attacker to already control the server, its database or its storage;
denial of service by sheer volume from many machines (put a rate-limiting proxy in front); a server run
without TLS in front of it on an untrusted network; the settings an operator chooses, such as open
registration.
