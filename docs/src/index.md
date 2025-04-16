# Rembus

```@meta
CurrentModule = Rembus
```

> A Component is a Broker or a Broker is a Component? This is the question.
> -- `The Rembus rebus`

Rembus is a Julia package designed for building distributed applications using
both Publish/Subscribe (Pub/Sub) and Remote Procedure Call (RPC) communication
patterns. 

A key distinguishing feature of Rembus is its highly flexible role
system, allowing a single application to act as a client, server, publisher,
subscriber, and even a message broker concurrently.

This unique capability enables the implementation of a wide range of distributed
architectures.

**Key Features:**

- Support multiple transport protocol: WebSocket, TCP, and ZeroMQ.
- Efficient CBOR encoding for primitive types.
- Optimized Arrow Table Format for encodings DataFrames.

**Application Roles:**

An application utilizing Rembus can assume one or more of the following roles:

- **RPC Client (Requestor):** Initiates requests for services from other components.
- **RPC Server (Exposer):** Provides and executes services in response to requests.
- **Pub/Sub Publisher:** Produces and disseminates messages to interested subscribers.
- **Pub/Sub Subscriber:** Consumes messages published on specific topics.
- **Broker:** Routes messages between connected components, potentially across different transport protocols.
- **Broker and Component:** Combines the routing capabilities of a broker with the application logic of a component.
- **Server:** Accepts connections from clients but does not route messages between them in the same way a broker does.

## Installation

```julia
using Pkg
Pkg.add("Rembus")
```

## Broker

A Rembus Broker acts as a central message router, facilitating communication
between components. Importantly, a Broker can bridge components using different
transport protocols (e.g., a ZeroMQ component can communicate with a WebSocket
component).

Starting a basic WebSocket Broker:

```julia
using Rembus

component() # Starts a WebSocket server listening on port 8000
```

The connection point for this broker is `ws://host:8000`.

A Broker can also function as a Component, connecting to another broker while
simultaneously acting as a local broker:

```julia
using Rembus
rb = component("ws://myhost:8000/mynode", ws=9000)
```

Here, the `mynode` component connects to the broker at `myhost:8000` and also
acts as a broker, accepting WebSocket connections on port `9000` and routing
messages between its connected components.

## Component

A Rembus Component is a process that embodies one or more of the communication
roles (Publisher, Subscriber, Requestor, Exposer). To connect to a broker, a
component uses a URL with the broker's connection point and a unique component
identifier:

```julia
component_url = "[<protocol>://][<host>][:<port>/][<cid>]"
```

Where `<protocol>` is one of `ws`, `wss`, `tcp`, `tls`, or `zmq`. `<host>` and
`<port>` specify the broker's address, and `<cid>` is the component's unique
name (optional for anonymous components).

Example connecting a named component:

```julia
rb = component("ws://host:8000/my_component")
```

A Component can also act as a Broker:

```julia
pub = component("ws://host:8000/my_pub", ws=9000)
```

The `my_pub` component communicates with the broker at `host:8000` and
simultaneously acts as a WebSocket broker on port `9000` for other components..

**Types of Components:**

- **Anonymous**: Assumes a random, ephemeral identity on each connection.
  Useful when message origin tracing isn't required, for subscribers
  uninterested in offline messages, and for prototyping.
- **Named**: Possesses a unique, persistent name, enabling it to receive messages
  published while offline.
- **Authenticated**: A named component with cryptographic credentials (private key
  or shared secret) to prove its identity, allowing access to private Pub/Sub
  topics and RPC methods.

## Server

Rembus simplifies the client-server architecture with a dedicated server API for
creating components that accept client connections without acting as
general-purpose message routers:

```julia
rb = server(ws=9876)
```

A server can expose RPC services and subscribe to Pub/Sub topics (typical server
roles) but can also publish messages or request RPC services from its connected
clients.

## A Simple Broker Script

```julia
#!/bin/bash
#=
SDIR=$( dirname -- "${BASH_SOURCE[0]}" )
BINDIR=$( cd -- $SDIR &> /dev/null && pwd )
exec julia -t auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using Rembus
Rembus.brokerd()
```

This script starts a Rembus broker with a default WebSocket server on port
`8000`. Use command-line arguments (e.g., `./broker -t 8001 -z 8002`) to enable
TCP and ZeroMQ transports.

```text
shell> ./broker
usage: broker [-n NAME] [-x] [-s] [-a] [-p HTTP] [-m PROMETHEUS]
              [-w WS] [-t TCP] [-z ZMQ] [-r POLICY] [-d] [-i] [-h]

optional arguments:
  -n, --name NAME       broker name (default: "broker")
  -x, --reset           factory reset, clean up broker configuration
  -s, --secure          accept wss and tls connections
  -a, --authenticated   only authenticated components allowed
  -p, --http HTTP       accept HTTP clients on port HTTP (type:
                        UInt16)
  -m, --prometheus PROMETHEUS
                        prometheus exposer port (type: UInt16)
  -w, --ws WS           accept WebSocket clients on port WS (type:
                        UInt16)
  -t, --tcp TCP         accept tcp clients on port TCP (type: UInt16)
  -z, --zmq ZMQ         accept zmq clients on port ZMQ (type: UInt16)
  -r, --policy POLICY   set the broker routing policy: first_up,
                        round_robin, less_busy (default: "first_up")
  -d, --debug           enable debug logs
  -i, --info            enable info logs
  -h, --help            show this help message and exit
```

See [Configuration](@ref) for customizing the runtime setting.  

## Index

```@index
```

```@autodocs
Modules = [Rembus]
```
