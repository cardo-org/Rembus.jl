# Rembus

![Rembus components](/docs/images/readme.png)

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://cardo-org.github.io/Rembus.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://cardo-org.github.io/Rembus.jl/dev/)
[![Build Status](https://github.com/cardo-org/Rembus.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/cardo-org/Rembus.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/cardo-org/Rembus.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/cardo-org/Rembus.jl)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)

Rembus is a middleware to implement high performance and fault-tolerant
distributed applications using RPC and Pub/Sub communication styles.

Data at Rest backend is powered by [DuckDB](https://duckdb.org/) offering
fast speed and powerful analitycal features.  

## Key Features

* Binary message encoding using [CBOR](https://cbor.io/).

* Built-in support for exchanging DataFrames.

* [DuckDB DuckLake](https://ducklake.select/) support.

* Pub/Sub QOS0, QOS1 and QOS2.

* Pub/Sub space topic routing and wildcard subscription (`*/*/temperature`).

* Macro-based API that make writing RPC and Pub/Sub applications simple and fast.

* MQTT integration.

* Multiple transport protocols: Tcp, Web Socket, ZeroMQ.

## Getting Started

```julia
using Pkg; Pkg.add("Rembus")
```

## Basic Concepts

The scope of Rembus is to facilitate communication between distributed
applications.

An application instrumented by Rembus uses the concept of `Component` to
abstract the two communication patterns:

* Remote Procedure Call (RPC): a client component invokes a function on a server
  component and waits for the result.
* Publish/Subscribe (Pub/Sub): a publisher component sends messages to a topic,
  and subscriber components receive messages from that topic.

A `Component` presents one or many of these roles:

* publish a message to a topic channel.
* subscribe to a topic channel to receive messages.
* expose a function to be invoked remotely.
* invoke a remote function on another component.
* route messages between components (broker role).

The are three factory constructors to create a `Component`, each one
reflecting its main role:

* `component(url::String)`: a component that connects to a broker or a server.

* `broker()`: a component that listen for connection requests and routes
   messages between components.

* `server()`: a component that listen for connection requests from others
  components. A server does not route messages between connected components.

## Component

Create a component that connects to `myhost.org` on port `8000` with the unique
name `mynode`:

```julia
using Rembus
node = component("ws://myhost.org:8000/mynode")
```

The `node` handle can be used to invoke remote functions and publish messages
to Pub/Sub topics.

```julia
# Invoke a remote function 'get_status' on the remote component
status = rpc(node, "get_status", Dict("verbose"=>true))

# Publish a message to the topic 'alerts/temperature' with a dictionary payload
publish(node, "alerts/temperature", Dict("value"=>75.0, "unit"=>"C"))
```

A remote component, named `myservices`, implements and exposes the `get_status`
method:

```julia
function get_status(options::Dict)
    if haskey(options, "verbose") && options["verbose"]
        return Dict(
            "status"=>"ok",
            "uptime"=>uptime(),
            "load"=>cpuload(),
            "mem_free"=>memfree()
        )
    else
        return Dict("status"=>"ok", "uptime"=>uptime())
    end
end

node = component("ws://myhost.org:8000/myservices")
expose(node, get_status)
wait(node)
```

To subscribe to Pub/Sub topics use the `subscribe` function:

```julia
function alert_handler(topic, payload; ctx=nothing, node=nothing)
    println("message from $topic: $payload")
end

subscribe(node, "alerts/temperature", alert_handler)
wait(node)
```

A subscriber function receives messages but not return a response like RPC
calls.

Note that both the exposer and the subscriber components must call `wait(node)`
to keep the component running and processing incoming requests.


## Broker

Start a broker component listening on the default WebSocket port `8000`:

```julia
using Rembus
bro = broker()

# Eventual broker custom logic
# ...

# Configure the broker to run forever until Ctrl-C
wait(bro)
```

> `broker()` is the same as `component()`: a component without an url
endpoint.

> `wait(bro)` is mandatory only when the broker is started in a script: in a
REPL session is a no-op.

See [broker documentation](docs/src/api.md#component) for more details on how to configure the broker.

The one-line shortcut for starting a broker from the terminal shell is:

```sh
julia -e "import Rembus; Rembus.brokerd()"
```

## Data at rest

Rembus has built-in support for persisting data using DuckDB
[DuckLake](https://ducklake.select/).

The DuckLake "analytical data lake" provides for fast-store and powerful query
features for large datasets.

The DuckLake storage backend can be configured using the `DUCKLAKE_URL`
environment variable and supports multiple database engines:

* DuckDB: the default backend for storing tabular data is a local DuckDB
  database file.
  
* Sqlite: `DUCKLAKE_URL="ducklake:sqlite:$HOME/.config/rembus/rembus.sqlite`

* Postgres: `DUCKLAKE_URL="ducklake:postgresql://user:password@host:port/rembus"`

If `DUCKLAKE_URL` is not defined then Rembus uses a local DuckDB database file
located at `$HOME/.config/rembus/rembus.duckdb`.

> For Postgres backend make sure to create the database `rembus` before starting
the broker.

A broker can be configured to persist Pub/Sub messages to a DuckLake storage
using a custom schema definition for the topics that are defined in the schema.

For example the following JSON formatted schema defines two tables: `sensor` and
`telemetry`.

`sensor` is a mote with a unique distinguish name `dn` that describe a `type` of
physical sensor deployed in a `site`

`telemetry` table store periodic `temperature` and `pressure` telemetry data
sent by the sensor motes.

```julia
json_string = """
{
    "tables": [
        {
            "table": "sensor",
            "topic": ":site/:type/:dn/sensor",
            "columns": [
                {"col": "site", "type": "TEXT", "nullable": false},
                {"col": "type", "type": "TEXT", "nullable": false},
                {"col": "dn", "type": "TEXT"}
            ],
            "keys": ["dn"]
        },
        {
            "table": "telemetry",
            "topic": ":dn/telemetry",
            "columns": [
                {"col": "dn", "type": "TEXT"},
                {"col": "temperature", "type": "DOUBLE"},
                {"col": "pressure", "type": "DOUBLE"}
            ],
            "extras": {"recv_ts": "ts", "slot": "time_bucket"}
        }
    ]
}
"""
```

The `sensor` table persists messages published to topics matching the
pattern `:site/:type/:dn/sensor` where `:site`, `:type` and `:dn` are dynamic
topic segments that are mapped to the corresponding table columns.

## Broker embedded with a Data Lake

With [DuckLake](https://ducklake.select/) enabled Rembus can persist and
retrieve Pub/Sub messages in batches directly from DuckDB.

```julia
# DuckDB use the package extension mechanism, so DuckDB MUST BE loaded first
# to enable DuckLake support.
using DuckDB
using Rembus

bro = broker(DuckDB.DB(), schema=json_string)
wait(bro)
```

Full example: [broker](examples/readme/broker.jl)

## Pub/Sub subscriber

Subscribe to the topic `**/telemetry` to receive all telemetry messages:

```julia
using Rembus

function telemetry(topic, payload; ctx=nothing, node=nothing)
    println("📡 telemetry on $topic: $payload")
end

meter = component("ws://localhost:8000/mymeter")

subscribe(meter, "**/telemetry", telemetry)

wait(meter)
```

Full example: [subscribe_telemetry.jl](examples/readme/subscribe_telemetry.jl)

## Pub/Sub publisher

Publish a message to the topic `belluno/HVAC/agordo.sala1/sensor` with an empty
payload:

```julia
using Rembus

pub = component("ws://localhost:8000/my_edge_gateway")
# ws, localhost and 8000 are the default values, so you can omit them
# pub = component("my_edge_gateway"))

publish(pub, "belluno/HVAC/agordo.sala1/sensor")
```

Full example: [publish_sensor.jl](examples/readme/publish_sensor.jl)

Publish a message to the topic `agordo.sala1/telemetry` with a dictionary
payload:

```julia
publish(
    pub,
    "agordo.sala1/telemetry",
    Dict("temperature" => 18.5, "pressure" => 1013.25)
)
```

Full example: [publish_telemetry.jl](examples/readme/publish_telemetry.jl)

## Query Data at Rest

For each table object defined in the `schema.json` are exposed two services, one
for querying and one for deleting data at rest:

* `query_{table}` for selecting items.

* `delete_{table}` for deleting items;

For example for getting the `telemetry` data at rest:

```julia
df = rpc(node, "query_telemetry", Dict("where"=>"dn like 'agordo/%'"))
```

Full example: [query_telemetry.jl](examples/readme/query_telemetry.jl)

## Rembus macro-based API

Rembus provides a macro-based API to simplify the development of distributed applications and
instrument julia functions with RPC and Pub/Sub capabilities.

### RPC server

```julia
@component "myserver"

function myservice(arg1)
    return "hello $arg1 💗"
end

@expose myservice

# Serve forever until Ctrl-C 
@wait
```

> The `@component` macro declares a unique name for the component that get known to the broker.
> On the broker side such identity permits to bind a twin operating on the behalf of the component either when it is offline.

### RPC client

```julia
response = @rpc myservice("rembus")
```

> When a name is not declared  with `@component` then a random uuid identifier is associated with the component each time the application starts.

### Pub/Sub subscriber

```julia
@component "myconsumer"

function mytopic(df::DataFrame)
    println("mean_a=$(mean(df.a)), mean_b=$(mean(df.b))")
end

@subscribe mytopic

# Receive messages forever until Ctrl-C 
@wait
```

### Pub/Sub publisher

```julia
df = DataFrame(a=1:1_000_000, b=rand(1_000_000))

# Fire and forget is the fastest publishing mechanism.
# at most once delivery guarantee.
@publish mytopic(df)

# Messages are acknowledged and eventually retransmitted.
# at least once delivery guarantee.
@publish mytopic(df) Rembus.QOS1

# Exactly once delivery guarantee.
@publish mytopic(df) Rembus.QOS2

```
