"""Messages on the document WebSocket (JSON text frames, one message per frame, `type` says which).

The first frame from the client must be `hello`; the server answers `welcome` or `error` and closes.
See docs/protocol.md for the flow.
"""
from __future__ import annotations

from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter

PROTOCOL_VERSION = 1


class _M(BaseModel):
    model_config = ConfigDict(extra="ignore")


# ------------------------------------------------------------------ client -> server
class Hello(_M):
    type: Literal["hello"] = "hello"
    protocol: int = PROTOCOL_VERSION
    token: str
    client_id: str = Field(min_length=8, max_length=64)  # one per running app; kept across reconnects
    format_version: int | None = None  # openswx file-format version of this client
    last_seq: int | None = None  # the last change this client has applied (reconnect); None: send the document
    app: str | None = None


class Change(_M):
    type: Literal["change"] = "change"
    change_id: str = Field(min_length=1, max_length=64)  # client-chosen; resending the same id is harmless
    base_seq: int  # the change was made on top of this sequence number
    ops: list[dict[str, Any]]
    command: str = ""  # what the user did, for the history ("extrude", "edit_feature")
    args: dict[str, Any] | None = None


class LockAcquire(_M):
    type: Literal["lock.acquire"] = "lock.acquire"
    request_id: str = ""
    targets: list[str]  # "f:<feature id>", "k:<top-level key>" or "structure"


class LockRelease(_M):
    type: Literal["lock.release"] = "lock.release"
    targets: list[str]


class Reserve(_M):
    type: Literal["ids.reserve"] = "ids.reserve"
    request_id: str = ""
    features: int = Field(default=0, ge=0, le=64)  # how many feature ids
    names: list[str] = Field(default_factory=list, max_length=64)  # name prefixes: "Sketch", "Boss-Extrude"


class Revert(_M):
    type: Literal["change.revert"] = "change.revert"
    request_id: str = ""
    seq: int
    mode: Literal["undo", "revert"] = "undo"  # undo: only your own change; revert: anyone's (editors)


class Presence(_M):
    type: Literal["presence"] = "presence"
    data: dict[str, Any]  # pointer, selection, tool, camera: relayed as is, never stored


class Draft(_M):
    type: Literal["draft"] = "draft"
    data: dict[str, Any] | None = None  # unfinished work to preview on other screens; None clears it


class RebuildReport(_M):
    type: Literal["rebuild.report"] = "rebuild.report"
    seq: int  # the document state the report is about
    errors: dict[str, str]  # feature id -> rebuild error


class Ping(_M):
    type: Literal["ping"] = "ping"


ClientMessage = Annotated[
    Union[Hello, Change, LockAcquire, LockRelease, Reserve, Revert, Presence, Draft, RebuildReport, Ping],
    Field(discriminator="type"),
]
client_message = TypeAdapter(ClientMessage)


# ------------------------------------------------------------------ server -> client
class Peer(_M):
    client_id: str
    user_id: str
    name: str
    colour: str
    read_only: bool = False
    presence: dict[str, Any] | None = None
    draft: dict[str, Any] | None = None


class LockHolder(_M):
    client_id: str
    user_id: str
    name: str
    detached: bool = False  # the holder is disconnected and the lock is in its grace period


class ChangeEvent(_M):
    type: Literal["change"] = "change"
    seq: int
    author: str  # user id
    author_name: str = ""
    client_id: str | None = None
    change_id: str | None = None  # lets a reconnecting client recognise its own changes in the catch-up
    time: float = 0.0
    ops: list[dict[str, Any]]
    command: str = ""
    reverts: int | None = None


class Welcome(_M):
    type: Literal["welcome"] = "welcome"
    protocol: int = PROTOCOL_VERSION
    document_id: str
    seq: int
    format_version: int | None = None
    you: Peer
    role: str
    read_only_reason: str | None = None
    peers: list[Peer] = Field(default_factory=list)
    locks: dict[str, LockHolder] = Field(default_factory=dict)
    broken: dict[str, dict[str, Any]] = Field(default_factory=dict)
    content: dict[str, Any] | None = None  # the whole document at `seq` ...
    changes: list[ChangeEvent] | None = None  # ... or the changes since the client's last_seq


class ChangeAccepted(_M):
    type: Literal["change.accepted"] = "change.accepted"
    change_id: str
    seq: int


class ChangeRejected(_M):
    type: Literal["change.rejected"] = "change.rejected"
    change_id: str
    # stale: someone changed the same thing since base_seq.  locked: someone else holds a lock.
    # invalid: cannot be applied.  read_only.  id_taken.  missing_blob.  too_large.
    code: str
    message: str = ""
    conflicts: dict[str, Any] = Field(default_factory=dict)  # target -> who / which change


class LockResult(_M):
    type: Literal["lock.result"] = "lock.result"
    request_id: str = ""
    granted: list[str] = Field(default_factory=list)
    denied: dict[str, LockHolder] = Field(default_factory=dict)


class LockChanged(_M):
    type: Literal["lock.changed"] = "lock.changed"
    acquired: dict[str, LockHolder] = Field(default_factory=dict)
    released: list[str] = Field(default_factory=list)


class Reserved(_M):
    type: Literal["ids.reserved"] = "ids.reserved"
    request_id: str = ""
    ids: list[str] = Field(default_factory=list)
    names: list[str] = Field(default_factory=list)


class RevertResult(_M):
    type: Literal["change.revert.result"] = "change.revert.result"
    request_id: str = ""
    ok: bool
    seq: int | None = None  # the new change that undid it
    ops: list[dict[str, Any]] | None = None  # what to apply locally (the sender gets no `change` event for it)
    code: str | None = None
    message: str = ""
    conflicts: dict[str, Any] = Field(default_factory=dict)


class PeerJoined(_M):
    type: Literal["peer.joined"] = "peer.joined"
    peer: Peer


class PeerLeft(_M):
    type: Literal["peer.left"] = "peer.left"
    client_id: str


class PresenceEvent(_M):
    type: Literal["presence"] = "presence"
    client_id: str
    data: dict[str, Any]


class DraftEvent(_M):
    type: Literal["draft"] = "draft"
    client_id: str
    data: dict[str, Any] | None = None


class RebuildStatus(_M):
    type: Literal["rebuild.status"] = "rebuild.status"
    seq: int
    broken: dict[str, dict[str, Any]]  # feature id -> {error, by_seq, by_user, by_name, command}


class VersionEvent(_M):
    type: Literal["version.created"] = "version.created"
    id: str
    seq: int
    name: str
    author: str


class ReferenceEvent(_M):
    """Sent to everyone in an assembly when a part it uses gets a new named version. Nothing about the part
    beyond its id: ask GET /documents/{assembly}/references, which tells each person what they may know."""
    type: Literal["reference.update"] = "reference.update"
    document: str  # the part
    components: list[str]  # ids of the components that are pinned to something older


class Pong(_M):
    type: Literal["pong"] = "pong"
    seq: int = 0


class Error(_M):
    type: Literal["error"] = "error"
    code: str
    message: str = ""


ServerMessage = Annotated[
    Union[Welcome, ChangeEvent, ChangeAccepted, ChangeRejected, LockResult, LockChanged, Reserved, RevertResult,
          PeerJoined, PeerLeft, PresenceEvent, DraftEvent, RebuildStatus, VersionEvent, ReferenceEvent, Pong, Error],
    Field(discriminator="type"),
]
server_message = TypeAdapter(ServerMessage)
