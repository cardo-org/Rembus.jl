# Rembus

```@meta
CurrentModule = Rembus
```

Rembus is a middleware for Pub/Sub and RPC communication styles.

There are two types of processes: Components and Brokers.

- A Component connect to a Broker;
- A Broker dispatch messages between Components;
- A Component expose RPC services and/or subscribe to Pub/Sub topics;
- A Component make RPC requests and/or publish messages to Pub/Sub topics;

## Installation

```julia
using Pkg
Pkg.add("Rembus")
```

Rembus installs and compiles in a minute or two.

## Broker

A `Broker` is a process that routes messages between components.

A `Broker` is capable of making components that use different transport protocols talk to each other. For example a component that uses a ZeroMQ socket may talk to a component
that uses the WebSocket protocol.

Starting a `Broker` is simple as:

```julia
using Rembus

caronte()
```

A startup script could be useful and the following `caronte` script will do:

```julia
##!/bin/bash
#=
BINDIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
exec julia --threads auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using Rembus

caronte()
```

`caronte` starts by default a WebSocket server listening on port 8000,
for enabling `tcp` and/or `zmq` transports use the appropriate arguments:

```text
shell> ./caronte
usage: caronte [-r] [-s] [-p HTTP] [-w WS] [-t TCP] [-z ZMQ] [-d] [-h]

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

## Component

A `Component` is a process that plays one or more of the following roles:

- Publisher (Pub/Sub) : produce messages;
- Subscriber (Pub/Sub): consume published messages;
- Requestor (RPC): request a service;
- Exposer (RPC): execute a service request and give back a response;

There are three type of components:

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

An URL string defines the identity and the connection parameters of a component. The [Macro-based API](./macro_api.md#component) page documents the URL format.

## Index

```@index
```

```@autodocs
Modules = [Rembus]
```
