# Change Log

## 0.6.0

- Prometheus integration

## 0.5.0 (25 November, 2024)

- Componenent multiplexing.

- Internals refatoring and bug fixes.

## 0.4.0 (17 October, 2024)

- Improve server api.

- Improve message persistence logic.

- Configurable authenticated and anonymous connection modes.

- [#32](https://github.com/cardo-org/Rembus.jl/issues/32) Update for julia 1.11.

## 0.3.0 (2 July, 2024)

- HTTP api (GET for RPC, POST for Pub/Sub).

- Fault-tolerant api: component(name) and api methods that talk to supervised task.

- Broker to broker routing (WebSocket only, experimental).

- rpc_future() and fetch_response(): APIs for asynchronous RPC.

## 0.2.1 (21 May, 2024)

- Add publish_interceptor method for extract and transform published messages.

## 0.2.0 (18 May, 2024)

- [#4](https://github.com/cardo-org/Rembus.jl/issues/4) Improve connection error logs .

- [#5](https://github.com/cardo-org/Rembus.jl/issues/4) Guarantee ordered by time delivery of published messages.

- Optimization: partial CBOR encode/decoding of messages at broker side.

- Full test coverage.

## 0.1.0 (25 March, 2024)

- First Release.
