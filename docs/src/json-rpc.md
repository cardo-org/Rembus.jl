# JSON-RPC

Rembus supports the
[JSON-RPC 2.0 specification](https://www.jsonrpc.org/specification).

To enable JSON-RPC interaction an HTTP endpoint must be explicity activated with
the `http` option:

```shell
bin/broker --http 9000
```

This starts an HTTP server on port 9000.
The root endpoint accepts JSON-RPC POST requests.

## RPC

In a JSON-RPC request:

- `method`: a string identifying the remote procedure (an exposed Rembus service).
- `params` (optional): structured arguments, provided either
  - by position as an Array, or
  - by name as an Object.
- `id`: required for requests expecting a response. The response object will
   contain the same id value.

### Example: Positional Parameters

Consider a service `subtract` that expects two positional arguments:

```julia
using Rembus

@expose subtract(x, y) = x - y
```

A valid JSON-RPC request would use an Array for `params`:

```bash
curl -X POST http://localhost:9000 -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0",
  "method": "subtract",
  "params": [42, 23],
  "id":2
}'
```

Response:

```json
{
  "jsonrpc":"2.0",
  "result":19,
  "id":2
}
```

### Example: Named Parameters

If the service expects keyword arguments:

```julia
using Rembus

@expose subtract(;x, y) = x - y
```

Then params must be an Object:

```bash
curl -X POST http://localhost:9000 -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0",
  "method": "subtract",
  "params": {"x":23, "y":42},
  "id":2
}'
```

Response:

```json
{
  "jsonrpc":"2.0",
  "result":-19,
  "id":2
}
```

### Example: Error Response

If the RPC call fails (e.g., missing required parameters), the response includes
an error object:

```bash
curl -X POST http://localhost:9000 -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0",
  "method": "subtract",
  "params": {"x":23},
  "id":2
}'
```

Response:

```json
{
  "jsonrpc":"2.0",
  "error":{
      "message":"UndefKeywordError(:y)",
      "code":-32000
  },
  "id":2
}
```

## Pub/Sub

In JSON-RPC, a Notification is a Request object without an id field.
Rembus uses this form to represent publish/subscribe messages.

For example, consider a subscriber function:

```julia
using Rembus

mytopic(val) = println("mytopic recv: $val")  

rb = component("my_node")
subscribe(rb, mytopic)
reactive(rb)
```

You can publish a message to the topic mytopic with a JSON-RPC Notification:

```bash
curl -X POST http://localhost:9000 -H "Content-Type: application/json" -d '{
  "jsonrpc": "2.0",
  "method": "mytopic",
  "params": ["hello world"]
}'
```

This will print:

```text
mytopic recv: hello world
```