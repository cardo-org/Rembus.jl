# Brokerless

It is possible to use Rembus protocol to setup a simple architecture without a broker.

In this scenario one component plays the role of a server that handles RPC requests and
receives messages published by others components.

In this scenario one component handles RPC requests and receives messages published by a component that connects directly to it.

> **NOTE** Without a broker a pub/sub is a one-to-one communication pattern: one component
publishes a message that is received by the components that embeds the server.

Below a minimal example of a component that exposes a service and accepts connections
for others components and respond only to authorized components:

```julia
using Rembus

function my_service(session, x,y)
    isauthenticated(session) || error("unauthorized")
    return x+y
end

function start_server()
    rb = server()
    provide(rb, my_service)
    serve(rb)
end

start_server()

```

## Detailed description

The component that play the server role is initialized as:

```julia
rb = server()
```

Bind methods implementation to the server with `provide`:

```julia
provide(rb, mymethod)
```

Only the `expose` method is required in a brokeless context because the only difference
between a rpc exposed method and a pub/sub subscribed method is that the first one replies
a value to the rpc client.

This means that one `expose` API works in place of `expose` and `subscribe` APIs required for configuring a broker.

The signature of `mymethod` must have a `session` object as first argument. The `session` object may be
useful for serving only authorized components with `isauthenticated(session)`.

For example if rpc method is invoked with two arguments:

```julia
@rpc myservice(x,y)
```

then the signature of `mymethod` must be:

```julia
function mymethod(session, x, y)
    return x + y
end
```

Finally step start the server and wait for ever for client connection requests:

```julia
Rembus.serve(rb)
```
