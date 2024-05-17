# Supervised API

The supervised API uses the metaprogramming features of Julia and provides something similar to a simple language extension.

The goal of this API is to make easy developing robust and fault-tolerant distributed applications.

The following macros comprise the API and enable Julia to be supercharged with the capabilities of a middleware for RPC and Pub/Sub messaging:

- [@component](#component)
- [@expose](#expose)
- [@unexpose](#unexpose)
- [@rpc](#rpc)
- [@subscribe](#subscribe)
- [@unsubscribe](#unsubscribe)
- [@publish](#publish)
- [@reactive](#reactive)
- [@unreactive](#reactive)
- [@forever](#forever)
- [@shared](#shared)
- [@rpc_timeout](#rpc_timeout)
- [@terminate](#terminate)

## component

A component needs to know the address of a broker, the transport protocol, the port
and optionally it has to declare a persistent unique name for the component.

These settings are defined with a URL string:

```julia
component_url = "[<protocol>://][<host>][:<port>/][<cid>]"

@component component_url
```

`<protocol>` is one of:

- `ws` web socket
- `wss` secure web socket
- `tcp` tcp socket
- `tls` TLS over tcp socket
- `zmq` ZeroMQ socket

`<host>` and `<port>` are the hostname/ip and the port of the listening broker.

`<cid>` is the unique name of the component. If it is not defined create an anonymous component.

For example:

```julia
@component "ws://caronte.org:8000/myclient"
```

defines the component `myclient` that communicates with the broker hosted on `caronte.org`, listening on port `8000` and accepting web socket connections.

> **NOTE** Rembus is "lazy": declaring a component does not open a connection to the broker.
> The connection will be opened when first needed.

### Default component URL parameters

The URL string may be simplified by using the enviroment variable `REMBUS_BASE_URL`.

Setting for example `REMBUS_BASE_URL=ws://localhost:8000` the above `component_url` may be simplified as:

```julia
@component "myclient"
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

@expose myservice
```

The exposed function will became available to RPC clients using the [`@rpc`](#rpc) macro.

## unexpose

Stop serving remote requests with [`@rpc`](#rpc) requests.

```julia
@unexpose myservice
```

## rpc

Request a remote method and wait for a response.

```julia
response = @rpc myservice(Dict("name"=>"foo", "tickets"=>3))
```

The arguments of the local function call `myservice` is transported to the remote site and `myservice` method expecting a `Dict` as argument is executed. 

The return value of `myservice` is transported back to the RPC client calling site
and `@rpc` returns.

If the remote method throws an Exception then the local RPC client throws an Exception reporting the reason of the remote error.

## subscribe

Declare interest for messages published on a logical channel that usually is
called topic.

The subscribed Julia methods are named as the topic of interest. 

```julia
function mytopic(x, y)
    @info "consuming x=$x and y=$y"
end

@subscribe mytopic

forever() # or until Ctrl-C 
```

By default `@subscribe` will consume messages published after the component connect
to the broker, messages sent previously are lost.

For receiving messages when the component was offline it is mandatory to set a component name and to declare interest in old messages with the option `before_now`:

```julia
@component "myname"

@subscribe mytopic before_now

forever() # or until Ctrl-C
```

> **NOTE** By design messages are not persisted until a component declares
interest for a topic. In other words the persistence feature for a topic is enabled at the time of first subscription. If is important not to loose any message the rule is subscribe first and publish after.

## unsubscribe

Stop the function to receive messages produced with [`@publish`](#publish).

```julia
@unsubscribe myservice
```

## publish

Publishing a message is like calling a local function named as the pub/sub topic. 

```julia
@publish mytopic(1.2, 3.0)
```

## reactive

Enable the reception of published messages from subscribed topics.

```julia
@reactive
```

Reactiveness is a property of a component and is applied to all subscribed topics.

By default a component starts with reactive mode enabled.

## unreactive

Stop receiving published messages.

```julia
@unreactive
```

## forever

Needed for components that [expose](#expose) and/or [subscribe](#subscribe) methods.
Wait forever for rpc requests or pub/sub messages.

By default `@forever` enable component reactiveness, see [@reactive](#reactive).

## shared

`@shared` is handy when a state must be shared between the subscribed methods, the exposed methods and the application.

Using a shared state implies that an additional argument must be passed to the methods.

For convention the first argument of a method that [@subscribe](#subscribe) or
[@expose](#expose) is the state object. 

The following example shows how to use a shared state:

- the struct `MyState` manages the state;
- the `@shared` macro binds the state object to the component;
- the subscribed and the exposed method must provide as first argument the state object;

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

@component "ingestor"
@shared mystate

# declare interest to messages produced with
# @publish add_metric(Dict("name"=>"pressure", "value"=>1.5))
@subscribe add_metric 

# implement a service that may be requested with
# @rpc fetch_metrics()
@expose fetch_metrics

forever()
```

## rpc_timeout

Set the maximum wait time for [@rpc](#rpc) requests.

```julia
@rpc_timeout value_in_seconds
```

By default the timeout is set to 5 seconds and may be changed using `REMBUS_TIMEOUT` 
environment variable.

## terminate

Close the network connection and shutdown the supervised process associated with the
component.   

```julia
@terminate
```
