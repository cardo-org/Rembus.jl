# HTTP Rest API

Rembus offers an HTTP REST interface for RPC requests, Pub/Sub publishing and a set of
broker administration commands.

An HTTP endpoint must be explicity activated with the `http` options:

```shell
bin/broker --http port
```

## APIs list

### No Auth and Basic Auth APIs

- GET   `http[s]://broker_host:port/{method_name}`
- POST  `http[s]://broker_host:port/{method_name}`
- POST  `http[s]://broker_host:port/subscribe/{topic}/{cid}`
- POST  `http[s]://broker_host:port/unsubscribe/{topic}/{cid}`
- POST  `http[s]://broker_host:port/expose/{topic}/{cid}`
- POST  `http[s]://broker_host:port/unexpose/{topic}/{cid}`

### Basic Auth APIs

- GET   `https://broker_host:port/admin/{command}`
- POST  `https://broker_host:port/private_topic/{topic}`
- POST  `https://broker_host:port/public_topic/{topic}`
- POST  `https://broker_host:port/authorize/{cid}/{topic}`
- POST  `https://broker_host:port/unauthorize/{cid}/{topic}`

Basic-Auth is required to invoke a REST endpoint that requires client authentication:

Since Basic-Auth send the password unprotected it is strongly advised to use an encrypted https
connection.

Basic-Auth POST example:

```julia
basic_auth(str::String) = Base64.base64encode(str)

auth = basic_auth("$admin:$password")
HTTP.post(
    "https://127.0.0.1:9000/private_topic/my_topic",
    ["Authorization" => auth]
)
```

## RPC

The GET HTTP method used to make RPC requests has the following url template:

GET `http[s]://broker_host:port/{method_name}`

A RPC uses a GET verb because GET is used by default to request data
from the server.

The body of the request is the JSON formatted list of arguments expected by the remote method or a JSON formatted value if the remote method expect a single argument.

The return value is JSON encoded in the response body.

For example, consider the following exposed methods by a server component:

```julia
using Rembus

@expose greet(name) = "hello $name"
@expose sum(x,y) = x + y
```

Then the HTTP invocations by a client will be:

```julia
using HTTP
using JSON3

sum_response = JSON3.read(
    HTTP.get("http://localhost:9000/sum", [], JSON3.write([1.0, 2.0])).body,
    Any
)

julia_response = JSON3.read(
    HTTP.get("http://localhost:9000/greet", [], JSON3.write("Julia")).body,
    Any
)

jane_response = JSON3.read(
    HTTP.get("http://localhost:9000/greet", [], JSON3.write(["Jane"])).body,
    Any
)
```

The RPC GET method returns a HTTP status success 200 and the returned value in the response body if the method succeeds or a HTTP status 403 and an error description in the response body if the method fails.

## Pub/Sub

The POST HTTP method used to publish a message has the following url template:

POST `http[s]://broker_host:port/{method_name}`

The POST verb is used for Pub/Sub because by default its scope it is to send data
to the server.

The body of the request is the JSON formatted list of arguments expected by the remote method or a JSON formatted value if the remote method expect a single argument, as in the case of RPC
method.

The Pub/Sub POST method returns a HTTP status success 200 and an empty response body, if the method succeeds.

## Subscribe and Expose configuration commands

The REST APIS:

- POST  `http[s]://broker_host:port/subscribe/{topic}/{component}`
- POST  `http[s]://broker_host:port/unsubscribe/{topic}/{component}`
- POST  `http[s]://broker_host:port/expose/{topic}/{component}`
- POST  `http[s]://broker_host:port/unexpose/{topic}/{component}`

may be used to configure in advance the "routing" tables of the broker, for example to
to cache Pub/Sub messages for components that never connected to the broker but that in
the future they will be interested to the topic messages.

## Authorization commands

Rembus topics come in two flawors:

- public topic accessible to all components.
- private topics accessible to authorized components.

The following REST commands set the privateness and authorize a component to access a private topic:

- POST `https://broker_host:port/private_topic/{topic}`
- POST `https://broker_host:port/authorize/{component}/{topic}`

The HTTP header must contain a Basic-Auth property with a base64 encoded string `component:password` associated with a component with admin privilege.

## Broker administration commands

The REST admin command set broker properties or return the broker configuration.

GET `https://broker_host:port/admin/{command}`

The following administrations `command` may be invoked:

- `broker_config`: return the components list that expose methods and subscribe to topics.
- `enable_debug`: set the broker log level to DEBUG.
- `disable_debug`: disable the DEBUG log level.
- `load_config`: reload the broker config files from disk.
- `save_config`: save the broker configuration to disk.
- `shutdown`: shutdown the broker.

For example the following set the broker log level to debug:

```julia
using Base64
using HTTP

basic_auth(str::String) = Base64.base64encode(str)

admin = "admin"
password = "aaa"

auth = basic_auth("$admin:$password")
HTTP.get(
    "http://127.0.0.1:9000/admin/enable_debug",
    ["Authorization" => auth]
)
```
