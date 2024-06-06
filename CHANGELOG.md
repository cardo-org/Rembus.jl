# Change Log

## 0.1.0 (25 March, 2024)

- First Release.

## 0.2.0 (18 May, 2024)

- [#4](https://github.com/cardo-org/Rembus.jl/issues/4) Improve connection error logs .

- [#5](https://github.com/cardo-org/Rembus.jl/issues/4) Guarantee ordered by time delivery of published messages.

- Optimization: partial CBOR encode/decoding of messages at broker side.

- Full test coverage.

## 0.2.1 (21 May, 2024)

- Add publish_interceptor method for extract and transform published messages.

## 0.3.0

- Broker to broker routing (WebSocket only, experimental).
