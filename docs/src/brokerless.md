# Client-Server architecture

It is possible to use Rembus protocol to setup a simple client-server
architecture without a decoupling broker.

In Rembus what is called a "client" is a component that connects and a "server
is a component that accepts connections. Apart from this, the client and server
are identical: the client can expose services and the server can call them.

> **NOTE** Without a broker a pub/sub is a one-to-one communication pattern: components
> publish messages that are received by the server but they are not broadcasted to anyone
> else.

In the below examples the server and the client plays the usual roles: the server exposes a service for handling RPC requests from the client.

## The Server

Below a minimal example of a component that exposes a service and accepts
connections for others components and respond only to authenticated components:

```julia
using Rembus

function my_service(x, y)
    return x+y
end

rb = server()
expose(rb, my_service)
wait(rb)
```

## The Client

On the calling side the client connects to the server and calls the service
exposed by the server. The client can also publish messages to the server.

```julia
using Rembus

rb = component("ws://localhost:8000/my_client")
response = rpc(rb, "my_service", x, y)
```
