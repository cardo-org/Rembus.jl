# Rembus

```@meta
CurrentModule = Rembus
```
Rembus is a middleware for Pub/Sub and RPC communication styles.

A Rembus node may play one o more roles: 

 - RPC client
 - RPC server
 - Pub/Sub publisher 
 - Pub/Sub subscriber
 - Broker

This meshup of roles enables a to implements a set of distributed architectures. 

## Installation

```julia
using Pkg
Pkg.add("Rembus")
```

## Broker

A `Broker` is a process that routes messages between components.

A `Broker` is capable of making components that use different transport protocols talk to each other. For example a component that uses a ZeroMQ socket may talk to a component
that uses the WebSocket protocol.

Starting a `Broker` is simple as:

```julia
using Rembus

broker()
```

Calling `broker` without arguments start by default a WebSocket server listening on port 8000.

A startup script could be useful and the following `broker` script will do:

```julia
#!/bin/bash
#=
SDIR=$( dirname -- "${BASH_SOURCE[0]}" )
BINDIR=$( cd -- $SDIR &> /dev/null && pwd )
exec julia -t auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using ArgParse
using Rembus

function command_line(default_name="broker")
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--name", "-n"
        help = "broker name"
        default = default_name
        arg_type = String
        "--reset", "-r"
        help = "factory reset, clean up broker configuration"
        action = :store_true
        "--secure", "-s"
        help = "accept wss and tls connections"
        action = :store_true
        "--authenticated", "-a"
        help = "only authenticated components allowed"
        action = :store_true
        "--http", "-p"
        help = "accept HTTP clients on port HTTP"
        arg_type = UInt16
        "--prometheus", "-m"
        help = "prometheus exposer port"
        arg_type = UInt16
        "--ws", "-w"
        help = "accept WebSocket clients on port WS"
        arg_type = UInt16
        "--tcp", "-t"
        help = "accept tcp clients on port TCP"
        arg_type = UInt16
        "--zmq", "-z"
        help = "accept zmq clients on port ZMQ"
        arg_type = UInt16
        "--policy"
        help = "set the broker routing policy"
        arg_type = Symbol
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
        "--info", "-i"
        help = "enable info logs"
        action = :store_true
    end
    return parse_args(s)
end

args = command_line()
name = args["name"]
if args["reset"]
    Rembus.broker_reset(name)
end

if args["debug"]
    Rembus.debug!()
elseif args["info"]
    Rembus.info!()
end

wait(Rembus.broker(
    name=name,
    ws=args["ws"],
    tcp=args["tcp"],
    zmq=args["zmq"],
    prometheus=args["prometheus"],
    authenticated=args["authenticated"]
))

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
