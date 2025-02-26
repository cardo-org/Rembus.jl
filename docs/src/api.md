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

Connect to the broker and return a Visor process handle used by the other APIs for exchanging data and commands.

In case of connection lost the underlying supervision logic attempts to reconnect
to the broker until it succeed.

```julia
rb = component("ws://hostname:8000/mycomponent")
```

The [Macro-based API](./macro_api.md#component) page documents the URL format.

## connect

Connect to the broker and return a connection handle used by the other APIs for exchanging data and commands.

The URL string passed to `connect` contains the address of a broker, the transport protocol, the port and optionally a persistent unique name for the component.

```julia
rb = connect("ws://hostname:8000/mycomponent")
```

The [Macro-based API](./macro_api.md#component) page documents the URL format.

## expose

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

The exposed function will became available to RPC clients using the [`@rpc`](./macro_api.md#rpc) macro or the [`rpc`](#rpc) function.

## unexpose

Stop serving remote requests via [`rpc`](#rpc) or [`@rpc`](./macro_api.md#rpc).

```julia
unexpose(rb, myservice)
```

## rpc

Request a remote method and wait for a response.

```julia
response = rpc(rb, "my_service", Dict("name"=>"foo", "tickets"=>3))
```

The service name and the arguments are CBOR-encoded and transported to
the remote site and the method `my_service` that expects a `Dict` as argument is called. 

The return value of `my_service` is transported back to the RPC client calling site
and taken as the return value of `rpc`.

If the remote method throws an Exception then the local RPC client will throw either an Exception reporting the reason of the remote error.

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

Declare interest for messages published on a logical channel: the topic.

The subscribed Julia methods are named as the topic of interest. 

```julia
function mytopic(x, y)
    @info "consuming x=$x and y=$y"
end

connect()

subscribe(rb, mytopic)

wait() # or until Ctrl-C 
```

By default `subscribe` will consume messages published after the component connect
to the broker, messages sent previously are lost.

For receiving messages when the component was offline it is mandatory to set a component name and to declare interest in old messages with the `from` argument set to `LastReceived()`:

```julia
connect("myname")

subscribe(rb, mytopic, from=LastReceived())

wait() # or until Ctrl-C
```

> **NOTE** By design messages are not persisted until a component declares
interest for a topic. In other words the persistence feature for a topic is enabled at the time of first subscription. If is important not to loose any message the rule is subscribe first and publish after.

The subscribed function will be called each time a component produce a message with the[`@publish`](./macro_api.md#publish) macro or the [`publish`](#publish) function.

## unsubscribe

Stop the function to receive messages produced with [`publish`](#publish) or
[`@publish`](./macro_api.md#publish).

```julia
unsubscribe(rb, myservice)
```

## publish

Publish a message:

```julia
rb = connect()

publish(rb, "metric", Dict("name"=>"trento/castello", "var"=>"T", "value"=>21.0))

close(rb)
```

`metric` is the message topic and the `Dict` value is the message content.

If the subscribed method expects many arguments send an array of values, where
each value is an argument:

```julia
# subscriber side
function my_topic(x,y,z)
    @assert x == 1
    @assert y == 2
    @assert z == 3
end

# publisher side
publish(rb, "my_topic", [1, 2, 3])
```

## reactive

Enable the reception of published messages from subscribed topics.

```julia
reactive(rb)
```

Reactiveness is a property of a component and is applied to all subscribed topics.

The [`wait`](#wait) function starts the loop that listen for published messages and by default the reactive mode is enabled.

## unreactive

Stop receiving published messages.

```julia
unreactive(rb)
```

## wait

Needed for components that [expose](#expose) and/or [subscribe](#subscribe) methods. Wait forever for rpc requests or pub/sub messages.

By default `wait` enable component reactiveness, see [reactive](#reactive).

## inject

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

wait()
```

## close

Close the network connection.

```julia
close(rb)
```

> NOTE: `close` applies to connections setup by [`connect`](#connect) api.

## shutdown

Close the network connection and shutdown the supervised process associated with the
component.

```julia
shutdown(rb)
```

> NOTE: `shutdown` applies to connections setup by [`component`](#component) api.
