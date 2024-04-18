# Unsupervised API

This API does not provide automatic reconnection in case of network
failures, if this happen the exception must be handled explicitly by the application.

The unsupervised API functions:

- [connect](#connect)
- [expose](#expose)
- [unexpose](#unexpose)
- [rpc](#rpc)
- [subscribe](#subscribe)
- [unsubscribe](#unsubscribe)
- [publish](#publish)
- [reactive](#reactive)
- [unreactive](#unreactive)
- [shared](#shared)
- [close](#close)

## connect

Connect to the broker and return a connection handle used by the other APIs for exchanging data and commands.

The URL string passed to `connect` contains the address of a broker, the transport protocol, the port and
optionally a persistent unique name for the component.

```julia
rb = connect("ws://hostname:8000/mycomponent")
```

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

The exposed function will became available to RPC clients using the [`@rpc`](#rpc) macro.

## unexpose

Stop serving remote requests with [`@rpc`](#rpc) requests.

```julia
unexpose(rb, myservice)
```

## rpc

Request a remote method and wait for a response.

```julia
response = rpc(rb, "myservice", Dict("name"=>"foo", "tickets"=>3))
```

The service name and the arguments are transported to the remote site and `myservice` method expecting a `Dict` as argument is executed. 

The return value of `myservice` is transported back to the RPC client calling site
and `rpc` returns.

If the remote method throws an Exception then the local RPC client will throw either an Exception reporting the reason of the remote error.

## subscribe

Declare interest for messages published on a logical channel: the topic.

The subscribed Julia methods are named as the topic of interest. 

```julia
function mytopic(x, y)
    @info "consuming x=$x and y=$y"
end

connect()

subscribe(rb, mytopic)

forever() # or until Ctrl-C 
```

By default `subscribe` will consume messages published after the component connect
to the broker, messages sent previously are lost.

For receiving messages when the component was offline it is mandatory to set a component name and to declare interest in old messages with the `retroactive` argument set to `true`:

```julia
connect("myname")

subscribe(rb, mytopic, true)

forever() # or until Ctrl-C
```

> **NOTE** By design messages are not persisted until a component declares
interest for a topic. In other words the persistence feature for a topic is enabled at the time of first subscription. If is important not to loose any message the rule is subscribe first and publish after.

## unsubscribe

Stop the function to receive messages produced with [`publish`](#publish).

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

## reactive

Enable the reception of published messages from subscribed topics.

```julia
reactive(rb)
```

Reactiveness is a property of a component and is applied to all subscribed topics.

By default a component starts with reactive mode enabled.

## unreactive

Stop receiving published messages.

```julia
unreactive(rb)
```
## shared

`shared` is handy when a state must be shared between the subscribed methods, the exposed methods and the application.

Using a shared state implies that an additional argument must be passed to the methods.

For convention the first argument of a method that [subscribe](#subscribe) or
[expose](#expose) is the state object. 

The following example shows how to use a shared state:

-  the struct `MyState` manages the state;
-  the `shared` method binds the state object to the component;
-  the subscribed and the exposed method must provide as first argument the state object;

```julia
mutable struct MyState
    counter::UInt
    data::Dict()
    MyState() = new(0, Dict())
end

mystate = MyState()

function add_metric(mystate, measure)
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
shared(rb, mystate)

# declare interest to messages produced with
# publish(rb, "add_metric", Dict("name"=>"pressure", "value"=>1.5))
subscribe(rb, add_metric) 

# implement a service that may be requested with
# rpc(rb, "fetch_metrics")
expose(rb, fetch_metrics)

forever()
```

## close

Close the network connection.   

```julia
close(rb)
```