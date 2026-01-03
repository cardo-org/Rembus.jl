# Changelog

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
