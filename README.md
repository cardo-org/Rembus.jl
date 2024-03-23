# Rembus

[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://cardo-org.github.io/Rembus.jl/stable/)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://cardo-org.github.io/Rembus.jl/dev/)
[![Build Status](https://github.com/cardo-org/Rembus.jl/actions/workflows/CI.yml/badge.svg?branch=main)](https://github.com/cardo-org/Rembus.jl/actions/workflows/CI.yml?query=branch%3Amain)
[![Coverage](https://codecov.io/gh/cardo-org/Rembus.jl/branch/main/graph/badge.svg)](https://codecov.io/gh/cardo-org/Rembus.jl)
[![Code Style: Blue](https://img.shields.io/badge/code%20style-blue-4495d1.svg)](https://github.com/invenia/BlueStyle)

Rembus is a middleware to implement high performance and fault-tolerant distributed applications.

## Key Features

* Built-in support for exchanging DataFrames.

* Macro-based API that make writing RPC and Pub/Sub applications simple and fast.

* Multiple transport protocols: Tcp, Web Socket, ZeroMQ.

* Binary message encoding using [CBOR](https://cbor.io/).

## The broker

Start the broker:

```sh
julia -e "using Rembus; caronte()"
```

## RPC server

```julia
@component "myserver"

function myservice(arg1)
    return "hello $arg1 💗"
end

@expose myservice

# Serve forever until Ctrl-C 
# in REPL forever() is not needed
forever()
```

> The `@component` macro declares a unique name for the component that get known to the broker.
> On the broker side such identity permits to bind a twin operating on the behalf of the component either when it is offline.

## RPC client

```julia
response = @rpc myservice("rembus")
```

> When a name is not declared  with `@component` then a random uuid identifier is associated with the component each time the application starts.

## Pub/Sub subscriber

```julia
@component "myconsumer"

function mytopic(df::DataFrame)
    println("mean_a=$(mean(df.a)), mean_b=$(mean(df.b))")
end

@subscribe mytopic

# forever() serves forever until Ctrl-C 
# in REPL forever() is not needed
forever()
```

## Pub/Sub publisher

```julia
df = DataFrame(a=1:1_000_000, b=rand(1_000_000))

@publish mytopic(df)
```
