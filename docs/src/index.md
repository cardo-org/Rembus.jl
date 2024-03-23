# Rembus

```@meta
CurrentModule = Rembus
```

## Broker

Starting the broker is simple as:

```sh
julia -e "using Rembus; caronte()"
```

Providing a startup script could be useful. The following `caronte` script suffice:

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

If `caronte` is in `PATH` then executing:

```sh
shell> caronte
```

starts the broker with setup controlled by the [Broker environment variables](@ref).  

## Component

A `Component` is a broker client who uses the Rembus protocol for RPC commands and
for streaming data in a Pub/Sub fashion.

The macro `@component` declares a component whom identity and the connection parameters are defined with an URL:

```julia
component_url = "[<protocol>://][<host>][:<port>/]<cid>"

@component component_url
```

`<protocol>` is one of:

- `ws` web socket
- `wss` secure web socket
- `tcp` tcp socket
- `tls` TLS over tcp socket
- `zmq` ZeroMQ socket

`<host>` and `<port>` are the hostname/ip and the port of the broker listening endpoint.

`<cid>` is the unique name of the component.

For example:

```julia
@component "ws://caronte.org:8000/myclient"
```

defines the component `myclient` that communicates with the broker hosted on `caronte.org`, listening on port `8000` and accepting web socket connections.

### Default component URL parameters

The string that define a component may be simplified by using the enviroment
variable `REMBUS_BASE_URL` that set the connection default parameters:

For example:

```sh
REMBUS_BASE_URL=ws://localhost:8000
```

define the default protocol, host and port, so that the above `component_url` may be simplified as:

```julia
@component "myclient"
```

Uses the web socket protocol to connect to `localhost` on port `8000`.

## Index

```@index
```

```@autodocs
Modules = [Rembus]
```
