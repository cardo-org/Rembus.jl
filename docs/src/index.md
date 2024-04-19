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

## Broker

A `Broker` is a process that routes messages between components.

A `Broker` is capable of making components that use different transport protocols talk to each other. For example a component that uses a ZeroMQ socket may talk to a component
that uses the WebSocket protocol.

Starting a `Broker` is simple as:

```sh
julia -e "using Rembus; caronte()"
```

A startup script could be useful and the following `caronte` script suffice:

```julia
##!/bin/bash
#=
BINDIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
exec julia --threads auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using Rembus

Rembus.caronte()
```

See [Broker environment variables](@ref) for customizing the runtime setting.  

## Component

A `Component` is a process that plays one or more of the following roles:

- Publisher (Pub/Sub) : produce messages;
- Subscriber (Pub/Sub): consume published messages;
- Requestor (RPC): request a service;
- Exposer (RPC): execute a service request and give back a response;

There are two flavors of components: anonymous and named ones.

An anonymous component assume a random and ephemeral identity each time it connects to the broker. Example usage for anonymous components may be:

- when it is not required to trace the originating source of messages;
- for a `Subscriber` when it is not required to receive messages published before the
  component goes online;
- for preliminary prototyping;

An URL string defines the identity and the connection parameters of a component. The [Supervised API](./supervised_api.md#component) page documents the URL format.

## Index

```@index
```

```@autodocs
Modules = [Rembus]
```
