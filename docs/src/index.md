# Rembus

```@meta
CurrentModule = Rembus
```

Rembus is a middleware for Pub/Sub and RPC communication styles.

A Rembus node may play one o more roles:

- RPC client - `component("ws://host:8000/my_rpc_client")`
- RPC server - `component("ws://host:8000/my_rpc_server")`
- Pub/Sub publisher - `component("ws://host:8000/my_producer")`
- Pub/Sub subscriber - `component("ws://host:8000/my_consumer")`
- Broker - `component(ws=8000)`
- Broker and Component - `component("my_app", ws=8000)`
- Server - `server(ws=8000)`

This meshup of roles enables a to implements a set of distributed architectures.

## Installation

```julia
using Pkg
Pkg.add("Rembus")
```

## Broker

A `Broker` routes messages between components.

A `Broker` is capable of making components that use different transport
protocols talk to each other. For example a component that uses a ZeroMQ socket
may talk to a component that uses the WebSocket protocol.

Starting a `Broker` is simple as:

```julia
using Rembus

component(ws=8000, tcp=8001, zmq=8002, prometheus=8003)
```

Calling `component` without arguments start by default a WebSocket server
listening on port 8000:

```julia
using Rembus
component()
```

The connection point of the **Broker** is defined by the url `ws://host:8000`.

A startup script could be useful and the following `broker` script will do:

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

`broker` starts by default a WebSocket server listening on port 8000,
for enabling `tcp` and/or `zmq` transports use the appropriate arguments:

```text
shell> ./broker
usage: broker [-r] [-s] [-p HTTP] [-w WS] [-t TCP] [-z ZMQ] [-d] [-h]

optional arguments:
  -r, --reset      factory reset, clean up broker configuration
  -s, --secure     accept wss and tls connections
  -p, --http HTTP  accept HTTP clients on port HTTP (type: UInt16)
  -w, --ws WS      accept WebSocket clients on port WS (type: UInt16)
  -t, --tcp TCP    accept tcp clients on port TCP (type: UInt16)
  -z, --zmq ZMQ    accept zmq clients on port ZMQ (type: UInt16)
  -d, --debug      enable debug logs
  -h, --help       show this help message and exit
```

See [Broker environment variables](@ref) for customizing the runtime setting.  

## Connected Components

Aside to be a **Broker** a component is a process that plays one or more of the following roles:

- Publisher (Pub/Sub) : produce messages;
- Subscriber (Pub/Sub): consume published messages;
- Requestor (RPC): request a service;
- Exposer (RPC): execute a service request and give back a response;

To connect to a broker a component must use a URL composed of the connection
point of the **Broker** and the component unique name.

```julia
component_url = "[<protocol>://][<host>][:<port>/][<cid>]"

rb = component(component_url)
```

`<protocol>` is one of:

- `ws` web socket
- `wss` secure web socket
- `tcp` tcp socket
- `tls` TLS over tcp socket
- `zmq` ZeroMQ socket

`<host>` and `<port>` are the hostname/ip and the port of the listening broker.

`<rid>` is the unique name of the component. If it is not defined an anonymous
component is created:

```julia
pub = component("ws://host:8000/my_pub")
```

defines the component `my_pub` that communicates with the broker hosted on
`host`, listening on port `8000` and accepting web socket connections.

### Types of Components

There are three type of Components:

- Anonymous
- Named
- Authenticated

An `Anonymous` component assume a random and ephemeral identity each time it connects to the broker. Example usage for anonymous components may be:

- when it is not required to trace the originating source of messages;
- for a `Subscriber` not interested to receive messages published when it was
  offline;
- for preliminary prototyping;

A `Named` component has a unique and persistent name that make possible to receive messages published when the component was offline.

An `Authenticated` component is a named component that own a private key or a shared secret which can prove its identity.

Only authenticated components may use Pub/Sub private topics and private RPC methods.

## Index

```@index
```

```@autodocs
Modules = [Rembus]
```
