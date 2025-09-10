const cid4macro = TaskLocalValue{String}(() -> "") # COV_EXCL_LINE

localcid() = cid4macro[]

localcid!(name) = cid4macro[] = name

"""
    broker(; <keyword arguments>)

Start a broker node and return a handle for interacting with it.

The broker acts as a central node to manage routing of RPC requests and Pub/Sub messages
between nodes.

It supports multiple communication protocols (WebSocket, TCP, and ZMQ) and allows for
customizable security, authentication, and routing policies.

### Keyword arguments
- `name::AbstractString="broker"`: The unique identifier for the broker supervisor process.
- `ws=nothing`: The WebSocket (ws/wss) listening port. Set to `nothing` to disable.
- `tcp=nothing`: The TCP (tcp/tls) listening port. Set to `nothing` to disable.
- `zmq=nothing`: The ZMQ Router listening port. Set to `nothing` to disable.
- `prometheus=nothing`: The Prometheus port for scraping monitoring metrics. Set to
  `nothing` to disable.
- `secure=false`: If `true`, enables WSS (WebSocket Secure) and TLS
  (Transport Layer Security) protocols for encrypted connections.
- `authenticated=false`: If `true`, only allows connections from named and authenticated
   nodes.
- `policy::String="first_up"`: The routing policy used when topics are served by multiple
   nodes. Possible options include:
    - `"first_up"`: Selects the first connected node from the list of nodes exposing the
      RPC method.
    - `"round_robin"`: Distributes requests evenly across nodes in a round-robin fashion.
    - `"less_busy"`: Chooses the node with fewer outstanding requests.

    ### Default Behavior
    If `ws`, `tcp`, and `zmq` are all set to `nothing`, the broker will default to listening
    for WebSocket connections on port `8000`.
        """
broker = component

"""
    server(; <keyword arguments>)

Start a server node and return a handle for interacting with it.

The server accepts connection from client nodes.

It supports multiple communication protocols (WebSocket, TCP, and ZMQ) and allows for
customizable security and authentication.

### Keyword arguments
- `name::AbstractString="broker"`: The unique identifier for the server supervisor process.
- `ws=nothing`: The WebSocket (ws/wss) listening port. Set to `nothing` to disable.
- `tcp=nothing`: The TCP (tcp/tls) listening port. Set to `nothing` to disable.
- `zmq=nothing`: The ZMQ Router listening port. Set to `nothing` to disable.
- `prometheus=nothing`: The Prometheus port for scraping monitoring metrics. Set to
  `nothing` to disable.
- `secure=false`: If `true`, enables WSS (WebSocket Secure) and TLS
  (Transport Layer Security) protocols for encrypted connections.
- `authenticated=false`: If `true`, only allows connections from named and authenticated
   nodes.

### Default Behavior
If `ws`, `tcp`, and `zmq` are all set to `nothing`, the broker will default to listening
for WebSocket connections on port `8000`.
"""
function server(;
    name::AbstractString="server",
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    prometheus=nothing,
    authenticated=false,
    secure=false
)
    if (isnothing(ws) && isnothing(tcp) && isnothing(zmq))
        ws = DEFAULT_WS_PORT
    end

    router = get_router(
        name=name,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        prometheus=prometheus,
        authenticated=authenticated,
        secure=secure,
        tsk=server_task
    )
    # Return a floating twin.
    return bind(router)
end

"""
    component(url::AbstractString; <keyword arguments>)

Start a component node and return a handle for interacting with it.

In case of connection lost the underlying supervision logic attempts to reconnect to the
broker until it succeed.

The `url` argument specifies the connection details for the component. For example,
the URL `ws://127.0.0.1:8000/foo` specifies:

- **Protocol**: `ws` (WebSocket). Other supported protocols: `wss`, `tcp`, `tls`, `zmq`.
- **Address**: `127.0.0.1` (localhost).
- **Port**: `8000`.
- **Component Name**: `foo`.

Anonymous connections omit the path part of the URL.

If not specified, Rembus considers the above values as the default values.

This means the URL `ws://127.0.0.1:8000/foo` is equivalent to simply `foo`.

Additionally, a component may listen for incoming connections on configured ports, enabling
it to act as a broker. These ports are specified using keyword arguments.

### Keyword Arguments
- `name=missing`:
  Unique string identifier for the component's supervisor process.
  Defaults to the path part of the `url` argument if `missing`.
- `ws=nothing`:
  WebSocket (ws/wss) listening port. Set to `nothing` to disable.
- `tcp=nothing`:
  TCP (tcp/tls) listening port. Set to `nothing` to disable.
- `zmq=nothing`:
  ZMQ Router listening port. Set to `nothing` to disable.
- `secure=false`: If `true`, enables WSS (WebSocket Secure) and TLS
  (Transport Layer Security) protocols for encrypted connections.
- `authenticated=false`: If `true`, only allows connections from named and authenticated
   nodes.
- `policy::String="first_up"`: The routing policy used when topics are served by multiple
   nodes. Options:
    - `"first_up"`: Selects the first connected node from the list of nodes exposing the
      RPC method.
    - `"round_robin"`: Distributes requests evenly across nodes in a round-robin fashion.
    - `"less_busy"`: Chooses the node with fewer outstanding requests.
- `enc=Rembus.CBOR`: wire message format for sending messages
   Set to `Rembus.JSON` for JSON-RPC-2.0 text encoding.
"""
function component(
    url::AbstractString;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR,
    failovers=[]
)
    uid = RbURL(url)
    return component(
        uid,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        http=http,
        name=name,
        authenticated=authenticated,
        policy=policy,
        secure=secure,
        enc=enc,
        failovers=failovers
    )
end

function component(;
    name::AbstractString="broker",
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    prometheus=nothing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR,
    failovers=[]
)
    if (isnothing(ws) && isnothing(tcp) && isnothing(zmq))
        ws = DEFAULT_WS_PORT
    end

    router = get_router(
        name=name,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        http=http,
        prometheus=prometheus,
        authenticated=authenticated,
        secure=secure,
        policy=policy,
    )
    set_policy(router, policy)
    twin = bind(router)
    twin.enc = enc
    return add_failovers(twin, failovers)
end

"""
$(TYPEDSIGNATURES)

Connect to the remote endpoint defined by `url`.

A disconnection from the remote endpoint will not trigger automatic reconnection.

# Example

rb = connect("ws://127.0.0.1:8000/mycomponent")
"""
connect(url::AbstractString; enc=CBOR)::Twin = connect(RbURL(url), enc=enc)

function issuccess(response)
    response = fetch(response.future)
    if response.status !== STS_SUCCESS
        return false
    end
    return true
end

"""
    inject(rb, ctx)

Bind a `ctx` context to the `rb` component.

When a `ctx` context is bound, the method signatures of subscribed and exposed
functions change as follows:

- the first argument is the `ctx` context.
- the second argument is the `rb` component.
- The remaining arguments correspond to the RPC request's arguments.

The `ctx` is useful for maintaining local state contextualized to the `rb` component.

### Example

```julia
using Rembus

# keep the number of processed messages
mutable struct Context
    msgcount::UInt
end

function topic(context::Context, rb, arg1, arg2)
    context.msgcount += 1
    some_logic(arg1, arg2)
end

ctx = Context(0)
rb = component("myname")
subscribe(rb, topic)
inject(rb, ctx)
```

In this example, when a message is published:

```julia
publish(rb, "topic", arg1, arg2)
```

The invoked method will receive the context and component as the first two arguments:

```julia
foo(container, rb, arg2, arg2)
```
"""
inject(rb::Twin, ctx=nothing) = rb.router.shared = ctx

"""
    expose(rb, topic::AbstractString, fn::Function)
    expose(rb, fn::Function)

Expose the methods of function `fn` to be executed by rpc clients using `topic` as
RPC method name.

If the `topic` argument is omitted the function name equals to the RPC method name.

`fn` returns the RPC response.

Expose the methods of function `fn` to be invoked by RPC clients, using `topic` as the RPC
method name.

If the `topic` argument is omitted, the function name is used as the RPC method name.

`fn` is expected to return the RPC response. Any exceptions thrown by `fn` are caught and
returned as an RPC exception.
"""
function expose(twin::Twin, name::AbstractString, func::Function)
    router = twin.router
    router.topic_function[name] = func
    msg = AdminReqMsg(twin, name, Dict{String,Any}(COMMAND => EXPOSE_CMD), rid(twin))
    return send_msg(twin, msg) |> fetch
end

expose(twin::Twin, func::Function) = expose(twin, string(func), func)

"""
    unexpose(rb, service::AbstractString)

Stop servicing RPC requests targeting `service`.
"""
function unexpose(twin::Twin, topic::AbstractString)
    router = twin.router
    delete!(router.topic_function, topic)
    msg = AdminReqMsg(twin, topic, Dict{String,Any}(COMMAND => UNEXPOSE_CMD))
    return send_msg(twin, msg) |> fetch
end

"""
    unexpose(rb, fn::Function)

Stop servicing RPC requests targeting `fn` function.
"""
unexpose(twin::Twin, fn::Function) = unexpose(twin, string(fn))

"""
    subscribe(rb, topic::AbstractString, fn::Function, from=Rembus.Now)
"""
function subscribe(
    twin::Twin,
    name::AbstractString,
    func::Function,
    from::Union{Real,Period,Dates.CompoundPeriod}=Now
)
    from_now = to_microseconds(from)
    router = twin.router
    router.topic_function[name] = func
    router.subinfo[name] = from_now

    msg = AdminReqMsg(
        twin,
        name,
        Dict{String,Any}(COMMAND => SUBSCRIBE_CMD, MSG_FROM => from_now),
        rid(twin)
    )
    return send_msg(twin, msg) |> fetch
end

"""
    subscribe(rb, fn::Function, from=Rembus.Now)

Subscribe to messages published to a `topic` and register a callback function `fn` to handle
incoming messages.

If the `topic` argument is omitted, the function name must be equal to the topic name.

The `from` (default=`Rembus.Now`) argument defines the starting point in time from which
messages published while the component was offline will be sent upon reconnection.
Possible `from` values:
  - **`Rembus.Now`**: Equivalent to `0.0`, ignores old messages, and starts receiving only
    new messages from now.
  - **`Rembus.LastReceived`**: Receives all messages published since the last disconnection.
  - **`n::Float64`**: Receives messages published within the last `n` seconds.
  - **`Dates.CompoundPeriod`**: Defines a custom period using a `CompoundPeriod` value.

To enable the reception of published messages, the `reactive` function must be called.

### Example

```julia
rb = component("myname")

# Define a callback function for the "mytopic" topic
function mytopic(data)
    println("Received: ", data)
end

# Subscribe to "mytopic", receiving only new messages from now
subscribe(rb, mytopic, from=Rembus.Now)
reactive(rb)
```
"""
function subscribe(
    twin::Twin,
    func::Function,
    from::Union{Real,Period,Dates.CompoundPeriod}=Now
)
    return subscribe(twin, string(func), func, from)
end

"""
    unsubscribe(rb, topic::AbstractString)

Stops delivering messages published on the specified `topic` to the `rb` component.
"""
function unsubscribe(twin::Twin, topic::AbstractString)
    router = twin.router
    delete!(router.topic_function, topic)
    delete!(router.subinfo, topic)
    msg = AdminReqMsg(twin, topic, Dict{String,Any}(COMMAND => UNSUBSCRIBE_CMD))
    return send_msg(twin, msg) |> fetch
end

"""
    unsubscribe(rb, fn::Function)

Stops delivering messages to the specified `fn` function.
"""
unsubscribe(twin::Twin, fn::Function) = unsubscribe(twin, string(fn))

"""
    reactive(rb, from::Union{Real,Period,Dates.CompoundPeriod}=Day(1))

Enable the reception of published messages for topics to which the node is subscribed via
[`subscribe`](@ref).

The `from` argument specifies the starting point in time from which messages published while
the component was offline will be delivered upon reconnection.

This value applies to all subscribed topics but can be overridden by the `from` argument in
the [`subscribe`](@ref) method for a specific topic — though only to define narrower time
ranges.

Possible `from` values:
  - **`Rembus.Now`**: Equivalent to `0.0`, ignores old messages, and starts receiving only
    new messages from now.
  - **`Rembus.LastReceived`**: Receives all messages published since the last disconnection.
  - **`n::Float64`**: Receives messages published within the last `n` seconds.
  - **`Dates.CompoundPeriod`**: Defines a custom period using a `CompoundPeriod` value.

### Example

```julia
rb = component("myname")
subscribe(rb, "mytopic1", Rembus.Now)
subscribe(rb, "mytopic2", Rembus.LastReceived)
subscribe(rb, "mytopic3", Hour(1))

reactive(rb, Day(1))
```

In this example:

-    `mytopic1` will receive messages starting from now.
-    `mytopic2` will receive messages published within the last day, even if `subscribe()`
     uses `Rembus.LastReceived`.
-    `mytopic3` will receive messages published within the last hour.
"""
function reactive(
    twin::Twin,
    from::Union{Real,Period,Dates.CompoundPeriod}=Day(1),
)
    msg = AdminReqMsg(
        twin,
        BROKER_CONFIG,
        Dict(
            COMMAND => REACTIVE_CMD,
            STATUS => true,
            MSG_FROM => to_microseconds(from))
    )
    twin.reactive = true
    return send_msg(twin, msg) |> fetch
end

"""
    unreactive(rb)

Stops the delivery of published messages to the `rb` component.
"""
function unreactive(twin::Twin)
    msg = AdminReqMsg(
        twin,
        BROKER_CONFIG,
        Dict(
            COMMAND => REACTIVE_CMD,
            STATUS => false
        )
    )
    twin.reactive = false
    return send_msg(twin, msg) |> fetch
end

"""
    publish(rb, topic::AbstractString, data...; qos=Rembus.QOS0)

Publish one or more data values to a `topic`.

Each element in `data` is passed as an argument to the remote method subscribed to
`topic`.

The `data` arguments may be of any type. For interoperability across components
implemented in different languages, the values should be either `DataFrame`s or primitive
types that are CBOR-encodable (see [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)).

The keyword argument `qos` specifies the quality of service (QoS) for message delivery:
- `Rembus.QOS0` (default): At most once delivery. A message may be delivered zero or one time.
- `Rembus.QOS1`: At least once delivery. A message is delivered one or more times.
- `Rembus.QOS2`: Exactly once delivery. A message is delivered once and only once.

# Examples

If the subscriber is a method that expects two arguments:

```julia
mytopic(x, y) = ...  # do something with x and y
```

You can publish a message with two data values::

```
rb = component("myname")
publish(rb, "mytopic", 1, 2)
```

If the remote subscribed method has no arguments, publish without data:

```
publish(rb, "mytopic")
```
"""
function publish(twin::Twin, topic::AbstractString, data...; qos=Rembus.QOS0)
    wait_open(twin) || failover_queue(twin) || error("connection down")
    msg = PubSubMsg(twin, topic, collect(data), qos)
    return publish_msg(twin, msg)
end

"""
    put(rb, topic::AbstractString, data...; qos=Rembus.QOS0)

Publish one or more data values to a channel identified by `topic`, which is automatically
prefixed with the component name.

Each element in `data` is passed as an argument to the remote method subscribed to
`topic`.

The `data` arguments may be of any type. For interoperability across components
implemented in different languages, the values should be either `DataFrame`s or primitive
types that are CBOR-encodable (see [RFC 8949](https://www.rfc-editor.org/rfc/rfc8949.html)).

The keyword argument `qos` specifies the quality of service (QoS) for message delivery:
- `Rembus.QOS0` (default): At most once delivery. A message may be delivered zero or one time.
- `Rembus.QOS1`: At least once delivery. A message is delivered one or more times.
- `Rembus.QOS2`: Exactly once delivery. A message is delivered once and only once.

# Examples

```
rb = component("myname")
put(rb, "mytopic", 1, 2)
```

This publishes the values 1 and 2 to the topic `myname/mytopic`.
"""
function put(twin::Twin, topic::AbstractString, data...; qos=Rembus.QOS0)
    wait_open(twin) || failover_queue(twin) || error("connection down")
    msg = PubSubMsg(twin, twin.uid.id * "/" * topic, collect(data), qos)
    return publish_msg(twin, msg)
end


"""
    rpc(rb, service::AbstractString, data...)

Make a request for a remote `service` method using Vararg `data` values as method arguments.

Return a value or throw an error if a request timeout occurs or the remotely invoked
method throws an exception.

### Example
```julia
rb = component("myclient")
# invoke mysum(1, 2) on the remote site
result = rpc(rb, "mysum", 1, 2)
```
"""
function rpc(
    twin::Twin,
    topic::AbstractString,
    data...
)
    wait_open(twin) || error("connection down")
    return fetch(fpc(twin, topic, collect(data)))
end

#  Just for reference, a way to implement rpc api with keyword arguments.
#
# """
#     rpc(rb, service::AbstractString; data...)
#
# Make a request for a remote `service` method using keyword arguments.
#
# `rpc(rb, "foo"; x=1, y=2.7, z=9)`
# """
# function rpc(
#     twin::Twin,
#     topic::AbstractString;
#     data...
# )
#     wait_open(twin) || error("connection down")
#     return fetch(fpc(twin, topic, Dict(data)))
# end

function direct(
    twin::Twin,
    target::AbstractString,
    topic::AbstractString,
    data...
)
    isopen(twin) || error("connection down")
    return fetch(fdc(twin, target, topic, data))
end

"""
    function authorize(rb, client::AbstractString, topic::AbstractString)

Authorize the `client` component to use the private `topic`.

The component must have the admin role for granting topic accessibility.
"""
function authorize(twin::Twin, client::AbstractString, topic::AbstractString)
    msg = AdminReqMsg(twin, topic, Dict(COMMAND => AUTHORIZE_CMD, CID => client))
    return send_msg(twin, msg) |> fetch
end

"""
    function unauthorize(rb, client::AbstractString, topic::AbstractString)

Revoke authorization to the `client` component for use of the private `topic`.

The component must have the admin role for revoking topic accessibility.
"""
function unauthorize(twin::Twin, client::AbstractString, topic::AbstractString)
    msg = AdminReqMsg(twin, topic, Dict(COMMAND => UNAUTHORIZE_CMD, CID => client))
    return send_msg(twin, msg) |> fetch
end

"""
    private_topic(rb, topic::AbstractString)

Set the specified `topic` to private.

The component must have the admin role to change the privacy level.
"""
function private_topic(twin::Twin, topic::AbstractString)
    msg = AdminReqMsg(twin, topic, Dict(COMMAND => PRIVATE_TOPIC_CMD))
    return send_msg(twin, msg) |> fetch
end

"""
    public_topic(rb, topic::AbstractString)

Set the specified `topic` to public.

The component must have the admin role to change the privacy level.
"""
function public_topic(twin::Twin, topic::AbstractString)
    msg = AdminReqMsg(twin, topic, Dict(COMMAND => PUBLIC_TOPIC_CMD))
    return send_msg(twin, msg) |> fetch
end

"""
    get_private_topics(rb)

Return a dictionary mapping private topics to their lists of authorized components.
"""
function get_private_topics(twin::Twin)
    msg = AdminReqMsg(twin, BROKER_CONFIG, Dict(COMMAND => PRIVATE_TOPICS_CONFIG_CMD))
    return send_msg(twin, msg) |> fetch
end

function admin(twin::Twin, command::Dict, topic=BROKER_CONFIG)
    msg = AdminReqMsg(twin, topic, command)
    return send_msg(twin, msg) |> fetch
end

#=
Future Request Call.
=#
function fpc(
    twin::Twin,
    topic::AbstractString,
    data=(),
    timeout=Inf
)
    msg = RpcReqMsg(twin, topic, data)
    return send_msg(twin, msg, timeout)
end

#=
Future Direct Call.
=#
function fdc(
    twin::Twin,
    target::AbstractString,
    topic::AbstractString,
    data=[]
)
    msg = RpcReqMsg(twin, topic, data, target)
    return send_msg(twin, msg)
end

function response_data(response)
    data = response.data
    if isa(data, IOBuffer)
        return eof(data) ? nothing : decode(data)
    else
        return data
    end
end

function Base.fetch(response::FutureResponse)
    res = fetch(response.future)
    close(response.timer)
    if isa(res, Exception)
        throw(res)
    end
    if res.status !== STS_SUCCESS
        if res.status == STS_CHALLENGE
            resend_attestate(last_downstream(res.twin.router), res.twin, res)
        else
            topic = nothing
            if isa(response.request, RembusTopicMsg)
                topic = response.request.topic
            end
            rembuserror(
                code=res.status, topic=topic, reason=response_data(res)
            )
        end
    elseif isa(res.data, IOBuffer)
        # data is an IOBuffer if transport is websocket or tcp
        return dataframe_if_tagvalue(decode(res.data))
    else
        return res.data
    end
end

function send_msg(twin, msg, timeout=Inf)
    router = last_downstream(twin.router)
    t = (timeout === Inf) ? router.settings.request_timeout : timeout
    timer = Timer(t) do tmr
        if haskey(twin.socket.direct, msg.id)
            put!(twin.socket.direct[msg.id].future, RembusTimeout("$msg timeout"))
            delete!(twin.socket.direct, msg.id)
        end
    end

    req = FutureResponse(msg, timer)
    twin.socket.direct[msg.id] = req
    if isa(twin.socket, Float)
        put!(twin.router.process.inbox, msg)
    else
        cast(twin.process, req)
    end
    return req
end

function publish_msg(twin, msg)
    if isa(twin.socket, Float)
        put!(twin.router.process.inbox, msg)
    else
        if failover_queue(twin)
            msg.counter = save_message(last_downstream(twin.router), msg)
        end
        cast(twin.process, msg)
    end

    return nothing
end

function rembuserror(; code, cid=nothing, topic=nothing, reason=nothing)
    if code == STS_METHOD_NOT_FOUND
        err = RpcMethodNotFound(topic)
    elseif code == STS_METHOD_EXCEPTION
        err = RpcMethodException(topic, reason)
    elseif code == STS_METHOD_LOOPBACK
        err = RpcMethodLoopback(topic)
    elseif code == STS_METHOD_UNAVAILABLE
        err = RpcMethodUnavailable(topic)
    else
        err = RembusError(code=code, topic=topic, reason=reason)
    end

    throw(err)
end

function broker_shutdown(admin::Twin)
    msg = AdminReqMsg(admin, "__config__", Dict(COMMAND => SHUTDOWN_CMD))
    return send_msg(admin, msg) |> fetch
end

"""
    request_timeout(rb)

Get the request timeout value for the component `rb`.
"""
request_timeout(twin::Twin) = last_downstream(twin.router).settings.request_timeout

"""
    request_timeout!(rb, value::Real)

Set the request timeout value for the component `rb`.
"""
function request_timeout!(twin::Twin, value::Real)
    router = last_downstream(twin.router)
    router.settings.request_timeout = value
    return twin
end

"""
    @publish topic(arg1,arg2,...)

Publish a message to `topic` logic channel.

The function `topic(arg1,arg2,...)` will be called on each connected component subscribed
to `topic`.

## Publisher
```julia
@publish foo("gfr", 54.2)
```

## Subscriber
```julia
function foo(name, value)
    println("do something with \$name=\$value")
end

@subscribe foo
@reactive

supervise()
```
"""
macro publish(topic, qos=Rembus.QOS0)
    ext = publish_expr(topic, qos)
    quote
        $(esc(ext))
    end
end

function publish_expr(topic, qos)
    ext = :(Rembus.publish(Rembus.Rembus.singleton(), t))
    fn = string(topic.args[1])
    ext.args[3] = fn
    args = topic.args[2:end]
    append!(ext.args, args)
    push!(ext.args, Expr(:kw, :qos, qos))
    return ext
end

"""
    @rpc service(arg1,...)

Call the remote `service` method and return its outcome.

The outcome may be the a return value or a [`RpcMethodException`](@ref) if the remote
throws an exception.

The `service` method must match the signature of an exposed remote `service` method.

Components may subscribe to `service` for receiving the `service` request.

## Exposer
```julia
function mymethod(x, y)
    return evaluate(x,y)
end

@expose mymethod
supervise()
```

## RPC client
```julia
response = @rpc mymethod(x,y)
```

## Subscriber
```julia
function service(x, y)
    ...
end

@subscribe service
@reactive

supervise()

```
"""
macro rpc(topic)
    ext = rpc_expr(topic)
    quote
        $(esc(ext))
    end
end

function rpc_expr(topic)
    ext = :(Rembus.rpc(Rembus.singleton(), t))
    fn = string(topic.args[1])
    ext.args[3] = fn
    args = topic.args[2:end]
    append!(ext.args, args)
    return ext
end

fnname(fn::Expr) = fn.args[1].args[1]
fnname(fn::Symbol) = fn

function expose_expr(fn)
    ext = :(Rembus.expose(Rembus.singleton(), t))
    ext.args[3] = fnname(fn)
    return ext
end

function subscribe_expr(fn, from)
    ext = :(Rembus.subscribe(Rembus.singleton(), t, from))
    ext.args[3] = fnname(fn)
    ext.args[4] = from.args[2]
    return ext
end

"""
    @expose fn

Expose all the methods of the function `fn`.

## Example

Expose the function `mycalc` that implements a service that may accept two numbers or a
string and number:

```julia
mycalc(x::Number, y::Number) = x+y
mycalc(x::String, y::Number) = length(x)*y

@expose mycalc
```
Call `mycal` service using the correct types of arguments:

```julia
# ok
julia> response = @rpc mycalc(1,2)
0x0000000000000003

# ok
julia> response = @rpc mycalc("hello",2.0)
10.0
```

If the RPC client call `mycalc` with the argument's type that
do not respect the signatures of the exposed service
then it throws [`RpcMethodException`](@ref)

```julia
julia> response = @rpc mycalc("hello","world")
ERROR: RpcMethodException("rembus", "mycalc", "MethodError: no method matching \
mycalc(::String, ::String) ...
```
"""
macro expose(fn::Symbol)
    ex = expose_expr(fn)
    quote
        $(esc(ex))
        nothing
    end
end

"""
    @expose function fn(arg1,...)
        ...
    end

Expose the function expression.
"""
macro expose(fn::Expr)
    ex = expose_expr(fn)
    quote
        $(esc(fn))
        $(esc(ex))
        nothing
    end
end

"""
    @unexpose fn

The methods of `fn` function is no more available to rpc clients.
"""
macro unexpose(fn)
    ext = :(Rembus.unexpose(Rembus.singleton(), t))
    ext.args[3] = fn
    quote
        $(esc(ext))
        nothing
    end
end

"""
    @subscribe topic [mode]

Setup a subscription to `topic` logic channel to handle messages from [`@publish`](@ref)
or [`@rpc`](@ref).

`mode` values`:
- `from_now` (default): receive messages published from now.
- `before_now`: receive messages published when the component was offline.

Messages starts to be delivered to `topic` when reactivity is enabled with `@reactive`
macro.

## Subscriber
```julia
function foo(arg1, arg2)
    ...
end

@subscribe foo
@reactive

supervise()
```

## Publisher
```julia
@publish foo("gfr", 54.2)
```
"""
macro subscribe(fn::Symbol, from=:(from = Rembus.Now))
    quote
        $(esc(subscribe_expr(fn, from)))
        nothing
    end
end

"""
    @subscribe function fn(args...)
        ...
    end [mode]

Subscribe the function expression.
"""
macro subscribe(fn::Expr, from=:(from = Rembus.Now))
    ex = subscribe_expr(fn, from)
    quote
        $(esc(fn))
        $(esc(ex))
        nothing
    end
end

"""
    @unsubscribe mytopic

`mytopic`'s methods stop to handle messages published to topic `mytopic`.
"""
macro unsubscribe(fn)
    ext = :(Rembus.unsubscribe(Rembus.singleton(), t))
    ext.args[3] = fn
    quote
        $(esc(ext))
        nothing
    end
end

"""
    @reactive

The subscribed methods start to handle published messages.
"""
macro reactive(from::Expr=:(from = Rembus.LastReceived))
    ext = :(Rembus.reactive(Rembus.singleton(), from))
    ext.args[3] = from.args[2]
    quote
        $(esc(ext))
        nothing
    end
end

"""
    @unreactive

The subscribed methods stop to handle published messages.
"""
macro unreactive()
    ext = :(Rembus.unreactive(Rembus.singleton()))
    quote
        $(esc(ext))
        nothing
    end
end

"""
     @inject container

Binds a `container` object, which is passed as the first argument to subscribed component
functions.

See [`inject`](@ref) for more details.
"""
macro inject(ctx)
    ext = :(Rembus.inject(Rembus.singleton(), ctx))
    ext.args[3] = ctx
    quote
        $(esc(ext))
        nothing
    end
end

"""
    @component "url"

Set the name of the component and the protocol for connecting
to the broker.

`url` may be:
- "myname": use \\\$REMBUS\\_BASE\\_URL for connection parameters
- "tcp://host:port/myname": tcp connection
- "ws://host:port/myname": web socket connection
- "zmq://host:port/myname": ZeroMQ connection
"""
macro component(name)
    quote
        cid = $(esc(name))
        component(cid)
        cid4macro[] = cid
    end
end

"""
    @wait

Block forever waiting for Ctrl-C/InterruptException or root supervisor shutdown.
"""
macro wait()
    quote
        isdefined(Visor.__ROOT__, :task) && wait(Visor.__ROOT__)
        return nothing
    end
end
