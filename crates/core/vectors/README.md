# Protocol v2 test vectors

Each `*.json` file is one WebSocket frame:

```json
{"name": "hello", "hex": "82a174...", "direction": "c2s", "json": {"t": "hello", "...": "..."}}
```

- `name`: the file name without `.json`.
- `hex`: the exact bytes of the frame (one binary WebSocket message), lower-case hex.
- `direction`: `c2s` is a frame a client sends (decode with `decode_client`), `s2c` one the server sends
  (`decode_server`).
- `json`: the same frame rendered as JSON. This is nothing but the frame's MessagePack with maps as
  objects, lists as arrays, `nil` as `null`, strings as strings and numbers as numbers. A float stays a
  float (`1.0` is not `1`). Object key order does not matter when comparing. The frames hold no binary
  values and no non-finite floats, so the rendering is lossless.

A test that reads these files checks that `msgpack.unpackb(bytes.fromhex(hex))` equals `json`, that the
decoder accepts the frame, and that encoding the decoded message gives back exactly `hex` (encoding is
deterministic: fixed field order, optional fields that are absent are left out).

The files are written by `cargo test -p oswx-core --test vectors -- --ignored write_vectors` from the
messages in `crates/core/tests/vectors.rs`. Change them only when the wire format changes on purpose.
