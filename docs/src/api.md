# Rembus API

This API provides both approaches to connection handling:

- automatic reconnection in case of network failures
- exception throwing in case of network errors and reconnection explicitly
  managed by the application.

Rembus API functions:

- [component](#component)
- [connect](#connect)
- [expose](#expose)
- [unexpose](#unexpose)
- [rpc](#rpc)
- [subscribe](#subscribe)
- [unsubscribe](#unsubscribe)
- [publish](#publish)
- [reactive](#reactive)
- [unreactive](#unreactive)
- [wait](#wait)
- [inject](#inject)
- [close](#close)
- [shutdown](#shutdown)

## component

```julia
component(
    url::AbstractString;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    failovers=[]
) -> Twin

# for more details
help?> component
```

Start a component and join the network of Rembus nodes.

### Connected Component

```julia
rb = component("ws://hostname:8000/mycomponent")
```

Connect to a broker that listens at the connection point `ws://hostname:8000`
and return the `rb` handle used by the other APIs for exchanging data and
commands.

In case of connection lost the underlying supervision logic attempts to reconnect
to the broker until it succeed.

See [Component](@ref) for URL format details.

### Broker

```juliaTYPEDSIGNATURES
rb = component(ws=8000, tcp=8001)
```

Start a broker that listens on the web socket port `8000` and on the TCP port
`8001`. The broker will accept connections from other components.

### Broker and Connected Component

This is an advanced pattern that allows to create a component that is also a
broker and that is able to connect to another broker. This pattern is useful for
creating a component that is able to act as a proxy between two brokers or to
create a component that is able to connect to a broker and at the same time
to act as a broker for other components.

```julia
rb = component("ws://hostname:8000/mycomponent", ws=9000)
```

Start a broker that listens on the WebSocket port `9000` and connect to a
broker defined at the connection point `ws://hostname:8000`.

## connect

```julia
connect(url::AbstractString) -> Twin
```

Connect to the broker and return a connection handle used by the other APIs for
exchanging data and commands.

The URL string passed to `connect` contains the address of a broker, the
transport protocol, the port and optionally a persistent unique name for the
component.

A disconnection from the remote endpoint will not trigger automatic
reconnection, for example:

```julia
rb = connect("ws://hostname:8000/mycomponent")
```

Connects to a broker that listens at the connection point
`ws://hostname:8000` and returns the `rb` handle used by the other APIs for
exchanging data and commands.

If the broker is not reachable the `connect` function will throw an Exception
and if the connection is lost at a later time the `rb` handle becomes
disconnected. The status of a component can be checked with the `isopen` 
method:

```julia
isopen(rb)
```

## expose

```julia
expose(rb, name::AbstractString, fn::Function)
expose(rb, fn::Function)
```

Take a Julia function and exposes all of its the methods.

```julia
function myservice(df::DataFrame)
    ...
    return another_dataframe
end

function myservice(map::Dict)
    ...
    return 0
end

expose(rb, myservice)
```

The exposed function will became available to RPC clients using the
[`@rpc`](./macro_api.md#rpc) macro or the [`rpc`](#rpc) function.

## unexpose

```julia
unexpose(rb, topic::AbstractString)
unexpose(rb, fn::Function)
```

Stop serving remote requests via [`rpc`](#rpc) or [`@rpc`](./macro_api.md#rpc).

## rpc

```julia
rpc(
    rb::Twin,
    service::AbstractString,
    data...
)

# for more details
help?> rpc
```

Request a remote method and wait for a response.

```julia
response = rpc(rb, "my_service", Dict("name"=>"foo", "tickets"=>3))
```

The service name and the arguments are CBOR-encoded and transported to
the remote site and the method `my_service` that expects a `Dict` as argument
is called.

The return value of `my_service` is transported back to the RPC client calling
site and taken as the return value of `rpc`.

If the remote method throws an Exception then the local RPC client will throw
either an Exception reporting the reason of the remote error.

If the exposed method expects many arguments send an array of values, where
each value is an argument:

```julia
# exposer side
function my_service(x,y,z)
    @assert x == 1
    @assert y == 2
    @assert z == 3
    return x+y+z
end

# rpc client side
rpc(rb, "my_service", [1, 2, 3])
```

## subscribe

```julia
subscribe(rb, topic::AbstractString, fn::Function, from=Rembus.Now)

subscribe(rb, fn::Function, from=Rembus.Now)

# for more details
help?> subscribe
```

Declare interest for messages published on the `topic` logical channel.

If the `topic` is not specified the function `fn` is subscribed to the topic
of the same name of the function.

The subscribed function will be called each time a component produce a message with the[`@publish`](./macro_api.md#publish) macro or the [`publish`](#publish) function.

To enable the reception of published messages, [`reactive`](@ref) must be
called.

```julia
function mytopic(x, y)
    @info "consuming x=$x and y=$y"
end

rb = connect()

subscribe(rb, mytopic)

reactive(rb) 
```

The `from` (default=`Rembus.Now`) argument defines the starting point in time from which
messages published while the component was offline will be sent upon reconnection.
Possible `from` values:
  - **`Rembus.Now`**: Equivalent to `0.0`, ignores old messages, and starts receiving only
    new messages from now.
  - **`Rembus.LastReceived`**: Receives all messages published since the last disconnection.
  - **`n::Float64`**: Receives messages published within the last `n` seconds.
  - **`Dates.CompoundPeriod`**: Defines a custom period using a `CompoundPeriod` value.

### Example

```julia
rb = connect("myname")

subscribe(rb, mytopic, Rembus.LastReceived)

reactive(rb) 
```

Receive all messages published since the last component disconnection.

## unsubscribe

```julia
unsubscribe(rb::Twin, topic::AbstractString)
unsubscribe(rb::Twin, fn::Function)
```

Stop the function to receive messages produced with [`publish`](#publish) or
[`@publish`](./macro_api.md#publish).

## publish

```julia
publish(rb::Twin, topic::AbstractString, data...; qos=Rembus.QOS0)
```

Publish (`Vararg`) data values to a specified `topic`.

Each item in `data` is mapped to an argument of the remote method subscribed to the `topic`.

The `data` values can be of any type. However, if the components are implemented in
different languages, the values must be either `DataFrames` or primitive types that are
CBOR-encodable (see [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)) for
interoperability.

The keywork argument `qos` defines the quality of service (QoS) for message delivery.
Possible values:

- `Rembus.QOS0`: (default): At most one message is delivered to the subscriber (message may be lost).
- `Rembus.QOS1`: At least one message is delivered to the subscriber (message may be duplicated).
- `Rembus.QOS2`: Exactly one message is delivered to the subscriber.

```julia
# subscriber side
function my_topic(x,y,z)
    @assert x == 1
    @assert y == 2
    @assert z == 3
end

# publisher side
publish(rb, "my_topic", 1, 2, 3)
```

## reactive

```julia
reactive(
    rb::Twin,
    from::Union{Real,Period,Dates.CompoundPeriod}=Day(1),
)

# for more details
help?> reactive
```

Enable the reception of published messages from subscribed topics.

Reactiveness is a property of a component and is applied to all subscribed topics.

## unreactive

```julia
unreactive(rb::Twin)
```

Stop receiving published messages.

## wait

```julia
wait(rb::Twin)
```

Needed for components that [expose](#expose) and/or [subscribe](#subscribe) 
methods. Wait forever for rpc requests or pub/sub messages.

## inject

```julia
inject(rb::Twin, state::Any)
```

Bind a state object to the component.

`inject` is handy when a state must be shared between the subscribed methods,
the exposed methods and the application.

When a state is injected two additional arguments are passed to the
subscribed/exposed methods:

- the first argument is the state value;
- the second argument is the node handle;

The following example shows how to use a shared state:

- the struct `MyState` manages the state;
- the `inject` method binds the state object to the component;
- the subscribed and the exposed method must declare as first argument the state
  object and as second argument the node handle;

```julia
mutable struct MyState
    counter::UInt
    data::Dict()
    MyState() = new(0, Dict())
end

mystate = MyState()

function add_metric(mystate::MyState, handle::RBHandle, measure)
    mystate.counter += 1 # count the received measures

    try
        indicator = measure["name"]
        mystate.data[indicator] = measure["value"]
    catch e
        @error "metrics: $e"
    end
end

function fetch_metrics(mystate)
    return mystate.data
end

rb = connect("ingestor")
inject(rb, mystate)

# declare interest to messages produced with
# publish(rb, "add_metric", Dict("name"=>"pressure", "value"=>1.5))
subscribe(rb, add_metric) 

# implement a service that may be requested with
# rpc(rb, "fetch_metrics")
expose(rb, fetch_metrics)

wait(rb)
```

## close

```julia
close(rb::Twin)
```

Close the network connections associated with the `rb` handle and terminate the supervised processes related to the handle.

```julia
close(rb)
```

## shutdown

```julia
shutdown(rb::Twin)
```

Terminate all the active supervised processes:
The method `shutdown(rb)` is equivalent to `close(rb)`.
