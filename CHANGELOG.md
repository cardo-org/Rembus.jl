# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.3.0] - Unreleased

### Added

- `mqtts://` URL scheme for connecting to an MQTT broker over TLS. The CA is
  taken from `HTTP_CA_BUNDLE` or `$REMBUS_DIR/ca`, and `rembus.crt` /
  `rembus.key` from the keystore (`REMBUS_KEYSTORE`) are used as client
  certificate when both are present.

- `upsert_<topic>` RPC service for topic in schema.

- When `overwrite_connection` is enabled, the replaced connection is closed
  with WebSocket close code 4000 (`"overwritten"`) and a component receiving
  it does not try to reconnect, avoiding two instances evicting each other in
  a loop.

- `Settings.max_ws_frame` (config key `max_ws_frame`, default `MAX_WS_FRAME` =
  60 MiB) sets the maximum WebSocket message size for both listeners and
  clients; it was previously HTTP.jl's 16 MiB default.

### Changed

- **Breaking:** a message with a single argument that is a list is published
  to MQTT wrapped in an array (`[[1, 2]]`), so that it is not mistaken for a
  list of arguments. An MQTT payload `[[1, 2]]` is received as a single list
  argument.

- **Breaking:** an empty MQTT payload is delivered as a message without
  arguments instead of being dropped, so a zero-argument message now
  round-trips through the MQTT bridge.

- **Breaking:** the encoding of a WebSocket connection is fixed by its first
  message (binary = CBOR, text = JSON). A later message of the other type
  closes the connection with code 1003 (`WS_CLOSE_UNSUPPORTED`). Previously
  the encoding was switched on every received message.

- **Breaking:** a JSON message whose `id` is a number is no longer accepted:
  ids are decimal strings. The id `"0"` is not valid either. Such a message is undecodable (close
  code 1008).

- **Breaking:** an empty, truncated or undecodable message (CBOR or JSON,
  including `NaN`/`Infinity` tokens or an invalid Rembus packet) closes the
  connection with WebSocket close code 1008. No `Response` is sent and the
  message is not ignored.

- Register: a key `type` other than `SIG_RSA` or `SIG_ECDSA` and a `pubkey`
  that is not a plain byte string (for example CBOR-tagged) are rejected with
  an error response. A CBOR-tagged `pin` no longer gets unwrapped.

- A Response packet without the `data` element (`[type, id, status]`) is
  accepted and handled as a Response with null data; when relayed it is
  emitted with a null `data` element.

- A broker forwards unknown trailing elements of Pub/Sub, RPC request and
  Response packets as elements of the relayed packet (the array length is
  preserved) instead of appending them as stray bytes after the array.

- Incoming CBOR packets: the type byte is accepted in any CBOR unsigned
  integer width (not only the direct and `0x18` forms); a value above 255 or
  a non-integer type is rejected as an invalid packet.

- The 8-byte message id is serialized in explicit little-endian byte order
  (previously the host's native order), matching rembus.python. The wire
  format is unchanged on little-endian hosts.

- Authenticated mode: anonymous components connected from the loopback
  interface (ws and tcp transports) are allowed to send commands and are not
  subject to the challenge timeout. Anonymous components connected from other
  addresses are still disconnected.

- MQTT bridge: the echo of a message published by the bridge itself is
  dropped, so subscribers receive a message published by a non-MQTT component
  once instead of twice. The echo is recognized by an MQTT 5 user property
  (`rembus-origin`) added to the published message, not by matching topic and
  payload: identical messages from external MQTT publishers are no longer
  dropped and late echoes are no longer delivered twice. Requires an MQTT 5
  broker that forwards user properties.

- MQTT bridge now follows rembus.python. MQTT → Rembus: the payload must be
  JSON; an array becomes the list of message arguments, any other value a
  single argument, and a payload that is not valid JSON is logged and
  dropped (it was previously delivered as a raw string). Rembus → MQTT: the
  payload is always JSON (a string argument is now quoted), several arguments
  are published as a JSON array (they were ignored), no arguments are
  published as a 0-length payload (they were not published), and the MQTT 5
  content type is `application/json`.

- `publish`/`put` with `qos=Rembus.QOS1` or `qos=Rembus.QOS2` now block until
  the ack is received (or the internal retries are exhausted), instead of
  returning immediately. If the ack is not received, an error is thrown.
  `qos=Rembus.QOS0` (the default) is unaffected and still returns immediately.

- **Breaking (JSON encoding):** the message type key of JSON-RPC messages is
  now `type` instead of `__type__`, as in rembus.python. The legacy
  `__type__` key is no longer accepted: Rembus.jl peers of a previous version
  cannot exchange JSON messages with this one. CBOR is unaffected.

- **Breaking:** the `pin` field of the `Register` message is now
  `SHA256(CBOR([pin, cid]))` (bytes in CBOR, base64 text in JSON) instead of
  the plaintext pin, so the pin is no longer sent in clear. Peers of a
  previous version cannot register with each other.

- msg_from: microsecond -> nanosecond

- Updated to HTTP.jl 2.x (built on the new "Reseau" transport/TLS stack).
  Notable adaptations required in Rembus and its test suite:
  - TLS trust is no longer picked up automatically from the `HTTP_CA_BUNDLE`
    environment variable; the CA bundle must now be configured explicitly
    when building an `HTTP.Client`.
  - `HTTP.Response.status` is now `Int64` (was `Int16`); code/tests comparing
    it with `===` against an `Int16` value must use `==` or compare against a
    plain `Int`.
  - `HTTP.get`/`HTTP.request` no longer accept a positional request body
    argument; use the keyword form instead.
  - Client-side `HTTP.Response.body` remains a plain `Vector{UInt8}`, while
    server-side request bodies inside a handler are an `HTTP.AbstractBody`
    (`EmptyBody`/`BytesBody`); the two must be handled differently when
    checking for emptiness or converting to `String`.
  - Raw `HTTP.WebSockets.open` connection failures now raise
    `Reseau.HostResolvers.OpError` (TCP connect failures) or
    `HTTP.TLSHandshakeError` (TLS handshake/certificate failures) instead of
    always wrapping in `HTTP.Exceptions.ConnectError`.

### Fixed

- The `Identity` message carries the bare component id as `cid` instead of
  the full `protocol://host:port/id` URL, as the Rembus protocol specifies.
  Peers that take `cid` literally (rembus.python) previously named the twin
  after the URL and mis-derived its tenant domain. Rembus.jl brokers still
  accept both forms.

- Server: a direct RPC whose target is the server itself, and a direct RPC
  coming from a broker peer, are no longer rejected with `"direct not
  permitted in server mode"`. The former is resolved with the server's own
  methods; direct RPCs from ordinary components to other targets are still
  rejected. This aligns with rembus.python.

- Authenticated mode: an anonymous component no longer waits for a challenge
  it cannot answer when `connection_mode` is `authenticated`.

- A server (`server_task`) now relays only the messages it publishes itself:
  messages published by a connected client are no longer broadcast to the
  other clients.

- A server replies `STS_METHOD_NOT_FOUND` to an RPC request that it cannot
  resolve with its own methods (previously no response was sent).

- Direct RPC (`direct`) is rejected with the error
  `direct not permitted in server mode` when the component is a server.

## [1.2.5] - 2026-03-17

### Fixed

- Push latest on DockerHub

## [1.2.4] - 2026-03-11

### Changed

- Changed the default ports for reducing the probability of conflict with other
  services:
  - 8338 -> 8338 (ws)
  - 8001 -> 8337 (tcp)
  - 8002 -> 8336 (zmq)

## [1.2.3] - 2026-02-09

### Added

- Add `wait()` reactive keyword: `wait(twin;reactive=true)`.

- Add `when` keyword to `query_*` payload.

- Add `REMBUS_FILESTORE` environment variable to enable file-based persistence.

### Changed

- Set `archiver_interval` default to 1 second.

### Fixed

- Restore topics spaces at startup

- DuckDB implementation fixes.

## [1.2.2] - 2026-01-03

### Changed

- Improve DuckDB extension.

## [1.2.1] - 2025-11-10

### Added

- Add Hierarchical topics similar to Zenoh

## [1.2.0] - 2025-11-1

### Added

- Support for DuckLake as storage backend.

- `slot` option for PubSub messages.

- Initial support for MQTT protocol.

### Changed

- Message id is now 8 bytes instead of 16 bytes.

## [1.1.1] - 2025-09-10

### Fixed

- In fpc() convert tuple to list for standard CBOR encoding.

## [1.1.0] - 2025-09-09

### Added

- [#53](https://github.com/cardo-org/Rembus.jl/issues/53) JSON_RPC 2.0 API

## [1.0.0] - 2025-07-20

### Added

- Prometheus integration.

### Changed

- Renamed function/macro `forever(rb)`/`@forever` to `wait(rb)`/`@wait`.

- `rpc`, `direct` and `publish` accepts varargs instead of a Vector of arguments.

### Removed

- macro `@rpc_timeout` is superseded by `Rembus.request_timeout!(newval)`

- Removed env variables `REMBUS_BROKER_PORT` and `REMBUS_DEBUG`.

## [0.5.0] - 2024-11-25

- Component multiplexing.

- Internals refactoring and bug fixes.

## [0.4.0] - 2024-10-17

- Improve server api.

- Improve message persistence logic.

- Configurable authenticated and anonymous connection modes.

- [#32](https://github.com/cardo-org/Rembus.jl/issues/32) Update for julia 1.11.

## [0.3.0] - 2024-07-02

- HTTP api (GET for RPC, POST for Pub/Sub).

- Fault-tolerant api: component(name) and api methods that talk to supervised task.

- Broker to broker routing (WebSocket only, experimental).

- rpc_future() and fetch_response(): APIs for asynchronous RPC.

## [0.2.1] - 2024-05-21

- Add publish_interceptor method for extract and transform published messages.

## [0.2.0] - 2024-05-18

- [#4](https://github.com/cardo-org/Rembus.jl/issues/4) Improve connection error logs .

- [#5](https://github.com/cardo-org/Rembus.jl/issues/4) Guarantee ordered by time delivery of published messages.

- Optimization: partial CBOR encode/decoding of messages at broker side.

- Full test coverage.

## [0.1.0] - 2024-03-25

- First Release.
