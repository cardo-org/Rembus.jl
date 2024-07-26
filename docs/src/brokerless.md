# Client-Server architecture

It is possible to use Rembus protocol to setup a simple client-server architecture without a decoupling broker.

In this scenario one component plays the role of a server that handles RPC requests and
receives messages published by others components that play the role of clients.

> **NOTE** Without a broker a pub/sub is a one-to-one communication pattern: components publish
> messages that are received by the server but they are not broadcasted to anyone else.

Below a minimal example of a component that exposes a service and accepts connections
for others components and respond only to authenticated components:

```julia
using Rembus

function my_service(ctx, component, x, y)
    ## authorization barrier 
    # isauthenticated(component) || error("unauthorized")
    return x+y
end

function start_server()
    rb = server()
    expose(rb, my_service)
    serve(rb)
end

start_server()
```

## Detailed description

### The Server

The component that plays the server role is initialized as:

```julia
rb = server()
```

`expose`, as usual,  make methods available to RPC clients:

```julia
expose(rb, my_service)
```

The signature of `my_service` must have a `ctx` value as first argument
and a `component` value as second argument:

```julia
function mymethod(ctx, component, x, y)
    return x + y
end
```

The `ctx` argument is a global state object that is passed to the server constructor:

```julia
mutable struct Ctx
    # state fields
end

ctx = Ctx()

rb = server(ctx)
```

If a global state is not needed by default ctx is set to `nothing`:

```julia
rb = server()
expose(rb, "my_service")

#implies that ctx is nothing:
function my_function(ctx, component, x, y)
    @assert ctx === nothing
end
```

The `component` object if useful for:

- serving only authenticated components;
- storing component session state into `session(component)` dictionary;

`serve` is the final step: the server starts and waits for connection requests
from clients:

```julia
Rembus.serve(rb)
```

### The Client

On the calling side the rpc method has to be invoked with two arguments:

```julia
@rpc my_service(x,y)
```

