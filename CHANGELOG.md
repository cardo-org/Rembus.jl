# Changelog

## 0.6.0 [2025-02-26]

### Added

- Prometheus integration

### Changed

- Renamed macro @wait to @wait

## 0.5.0 [2024-11-25]

- Componenent multiplexing.

- Internals refatoring and bug fixes.

## 0.4.0 [2024-10-17]

- Improve server api.

- Improve message persistence logic.

- Configurable authenticated and anonymous connection modes.

- [#32](https://github.com/cardo-org/Rembus.jl/issues/32) Update for julia 1.11.

## 0.3.0 [2024-07-02]

- HTTP api (GET for RPC, POST for Pub/Sub).

- Fault-tolerant api: component(name) and api methods that talk to supervised task.

- Broker to broker routing (WebSocket only, experimental).

- rpc_future() and fetch_response(): APIs for asynchronous RPC.

## 0.2.1 [2024-05-21]

- Add publish_interceptor method for extract and transform published messages.

## 0.2.0 [2024-05-18]

- [#4](https://github.com/cardo-org/Rembus.jl/issues/4) Improve connection error logs .

- [#5](https://github.com/cardo-org/Rembus.jl/issues/4) Guarantee ordered by time delivery of published messages.

- Optimization: partial CBOR encode/decoding of messages at broker side.

- Full test coverage.

## 0.1.0 [2024-03-25]

- First Release.
