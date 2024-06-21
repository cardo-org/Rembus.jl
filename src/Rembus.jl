#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#
module Rembus

using ArgParse
using Arrow
using Base64
using CSV
using DocStringExtensions
using DataFrames
using Dates
using DataStructures
using FileWatching
using HTTP
using JSON3
using Logging
using MbedTLS
using Random
using Reexport
using Sockets
using Parameters
using PrecompileTools
using Preferences
using Printf
using URIs
using Serialization
using UUIDs
@reexport using Visor
using ZMQ

export @component
export @enable_ack, @disable_ack
export @expose, @unexpose
export @subscribe, @unsubscribe
export @rpc
export @publish
export @reactive, @unreactive
export @shared
export @rpc_timeout
export @forever
export @terminate

# rembus client api
export connect
export isauthenticated
export server
export serve
export expose, unexpose
export subscribe, unsubscribe
export direct
export rpc
export publish
export reactive, unreactive
export enable_ack, disable_ack
export authorize, unauthorize
export private_topic, public_topic
export provide
export close
export isconnected
export rembus
export shared
export set_balancer
export forever

# broker api
export caronte, session, republish, msg_payload

export RembusError
export RembusTimeout
export RpcMethodNotFound, RpcMethodUnavailable, RpcMethodLoopback, RpcMethodException
export SmallInteger

struct Component
    id::String
    protocol::Symbol
    host::String
    port::UInt16
    props::Dict{String,String}

    function Component(url::String)
        baseurl = get(ENV, "REMBUS_BASE_URL", "ws://127.0.0.1:8000")
        baseuri = URI(baseurl)
        uri = URI(url)
        props = queryparams(uri)

        host = uri.host
        if host == ""
            host = baseuri.host
        end

        portstr = uri.port
        if portstr == ""
            portstr = baseuri.port
        end

        port = parse(UInt16, portstr)

        proto = uri.scheme
        if proto == ""
            name = uri.path
            protocol = Symbol(baseuri.scheme)
        elseif proto in ["ws", "wss", "tcp", "tls", "zmq"]
            name = startswith(uri.path, "/") ? uri.path[2:end] : uri.path
            protocol = Symbol(proto)
        else
            error("wrong url $url: unknown protocol $proto")
        end
        if isempty(name)
            name = "rembus"
        end
        return new(name, protocol, host, port, props)
    end
end

brokerurl(c::Component) = "$(c.protocol == :zmq ? :tcp : c.protocol)://$(c.host):$(c.port)"

abstract type RBHandle end

mutable struct RBConnection <: RBHandle
    shared::Any
    socket::Any
    msgch::Channel
    reactive::Bool
    client::Component
    receiver::Dict{String,Function}
    subinfo::Dict{String,Bool}
    out::Dict{UInt128,Threads.Condition}
    acktimer::Dict{UInt128,Timer}
    context::Union{Nothing,ZMQ.Context}
    RBConnection(name::String) = new(
        missing,
        nothing,
        Channel(MESSAGE_CHANNEL_SZ),
        false,
        Component(name),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        nothing
    )
    RBConnection(client=getcomponent()) = new(
        missing,
        nothing,
        Channel(MESSAGE_CHANNEL_SZ),
        false,
        client,
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        nothing
    )
end

Base.isless(rb1::RBConnection, rb2::RBConnection) = length(rb1.out) < length(rb2.out)

function Base.show(io::IO, rb::RBConnection)
    return print(io, "client [$(rb.client.id)], isconnected: $(isconnected(rb))")
end

mutable struct RBPool <: RBHandle
    last_invoked::Dict{String,Int} # topic => index of last used connection
    connections::Vector{RBConnection}
    RBPool(conns::Vector{RBConnection}=[]) = new(Dict(), conns)
end

include("constants.jl")
include("configuration.jl")
include("logger.jl")
include("cbor.jl")
include("encode.jl")
include("decode.jl")
include("protocol.jl")
include("broker.jl")
include("transport.jl")
include("admin.jl")
include("store.jl")
include("register.jl")

function __init__()
    setup(CONFIG)
    Visor.setroot(intensity=3)
    atexit(shutdown)
end

struct CloseConnection end

# Wrong tcp packet received.
struct WrongTcpPacket <: Exception
end

# A message error received from the broker.
abstract type RembusException <: Exception end

# An error response from the broker that is not one of:
# STS_METHOD_NOT_FOUND, STS_METHOD_EXCEPTION, STS_METHOD_LOOPBACK, STS_METHOD_UNAVAILABLE
Base.@kwdef struct RembusError <: RembusException
    code::UInt8
    cid::Union{String,Nothing} = nothing
    topic::Union{String,Nothing} = nothing
    reason::Union{String,Nothing} = nothing
end

struct AlreadyConnected <: RembusException
    cid::String
end

"""
`RpcMethodNotFound` is thrown from a rpc request when the called method is unknown.

fields:
$(FIELDS)

## RPC Client
```julia
@rpc coolservice()
```
Output:
```
ERROR: Rembus.RpcMethodNotFound("rembus", "coolservice")
Stacktrace:
...
```
"""
struct RpcMethodNotFound <: RembusException
    "component name"
    cid::String
    "service name"
    topic::String
end

"""
    RpcMethodUnavailable

Thrown when a RPC method is unavailable.

A method is considered unavailable when some component that exposed the method is
currently disconnected from the broker.

# Fields
$(FIELDS)
"""
struct RpcMethodUnavailable <: RembusException
    "component name"
    cid::String
    "service name"
    topic::String
end

"""
    RpcMethodLoopback

Thrown when a RPC request would invoke a locally exposed method.

# Fields
$(FIELDS)
"""
struct RpcMethodLoopback <: RembusException
    "component name"
    cid::String
    "service name"
    topic::String
end

"""
    RpcMethodException

Thrown when a RPC method throws an exception.

# Fields
$(FIELDS)

## Exposer
```julia
@expose foo(name::AbstractString) = "hello " * name
```
## RPC client
```julia
try
    @rpc foo(1)
catch e
    @error e.reason
end
```
Output:
```
┌ Error: MethodError: no method matching foo(::UInt64)
│
│ Closest candidates are:
│   foo(!Matched::AbstractString)
│    @ Main REPL[2]:1
└ @ Main REPL
```
"""
struct RpcMethodException <: RembusException
    "component name"
    cid::String
    "service name"
    topic::String
    "remote exception description"
    reason::String
end

"""
    RembusTimeout

Thrown when a response it is not received.
"""
struct RembusTimeout <: RembusException
    msg::String
    RembusTimeout(msg) = new(msg)
end

function rembuserror(raise::Bool=true; code, cid=nothing, topic=nothing, reason=nothing)
    if code == STS_METHOD_NOT_FOUND
        err = RpcMethodNotFound(cid, topic)
    elseif code == STS_METHOD_EXCEPTION
        err = RpcMethodException(cid, topic, reason)
    elseif code == STS_METHOD_LOOPBACK
        err = RpcMethodLoopback(cid, topic)
    elseif code == STS_METHOD_UNAVAILABLE
        err = RpcMethodUnavailable(cid, topic)
    else
        err = RembusError(code=code, cid=cid, topic=topic, reason=reason)
    end

    if raise
        throw(err)
    else
        return err
    end
end

struct CastCall
    topic::String
    data::Any
end

Base.show(io::IO, call::CastCall) = print(io, call.topic)

rembus_dir() = joinpath(CONFIG.root_dir, "rembus")

request_timeout() = parse(Float32, get(ENV, "REMBUS_TIMEOUT", "10"))

connect_request_timeout() = parse(Float32, get(ENV, "REMBUS_CONNECT_TIMEOUT", "10"))

getcomponent() = Component(Rembus.CONFIG.cid)

function name2proc(name::AbstractString, startproc=false, setanonymous=false)
    cmp = Component(name)
    if from(cmp.id) === nothing
        throw(ErrorException("unknown process $(cmp.id)"))
    end
    return name2proc(Component(name), startproc, setanonymous)
end

function name2proc(::Nothing, startproc=false, setanonymous=false)
    return name2proc(getcomponent(), startproc, setanonymous)
end

function name2proc(cmp::Component, startproc=false, setanonymous=false)
    proc = from(cmp.id)
    if proc === nothing
        if setanonymous && CONFIG.cid == "rembus"
            proc = startup(rembus())
        end
    end
    if startproc && !isdefined(proc, :task)
        Visor.startchain(proc)
    end

    return proc
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
        Rembus.CONFIG.cid = $(esc(name))
        Visor.startup(rembus())
    end
end

Visor.shutdown(nothing) = nothing

"""
    @terminate

Close the connection and terminate the component.
"""
macro terminate(name=nothing)
    quote
        shutdown(name2proc($(esc(name))))
        Rembus.CONFIG.cid = "rembus"
        nothing
    end
end

"""
    @rpc_timeout value

Set the rpc request timeout in seconds.
"""
macro rpc_timeout(value)
    quote
        ENV["REMBUS_TIMEOUT"] = $(esc(value))
    end
end

macro rembus(cid=nothing)
    quote
        startup(rembus($(esc(cid))))
    end
end

function holder_expr(shared, cid=nothing)
    ex = :(call(
        Rembus.name2proc("cid", true, true),
        Rembus.SetHolder(aaa),
        timeout=Rembus.request_timeout()
    ))
    ex.args[3].args[2] = shared
    ex.args[2].args[2] = cid
    return ex
end

"""
     @shared container

Bind a `container` object that is passed as the first argument of the subscribed
component functions.

The `container` is useful for mantaining a state.

```julia
using Rembus

# keep the number of processed messages
mutable struct Context
    msgcount::UInt
end

function topic(context::Context, arg1, arg2)
    context.msgcount += 1
    some_logic(arg1, arg2)
end

ctx = Context(0)
@subscribe topic
@shared ctx
```

Using `@shared` to set a `container` object means that if some component
`publish topic(arg1,arg2)` then the method `foo(container,arg2,arg2)` will be called.

"""
macro shared(container)
    ex = holder_expr(container)
    quote
        $(esc(ex))
        nothing
    end
end

macro shared(cid, container)
    ex = holder_expr(container, cid)
    quote
        $(esc(ex))
        nothing
    end
end

function publish_expr(topic, cid=nothing)
    ext = :(cast(Rembus.name2proc("cid", true, true), Rembus.CastCall(t, [])))

    fn = string(topic.args[1])
    ext.args[2].args[2] = cid
    ext.args[3].args[2] = fn
    args = topic.args[2:end]
    ext.args[3].args[3].args = args
    return ext
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
macro publish(topic)
    ext = publish_expr(topic)
    quote
        $(esc(ext))
    end
end

macro publish(cid, topic)
    ext = publish_expr(topic, cid)
    quote
        $(esc(ext))
    end
end

function rpc_expr(topic, cid=nothing)
    ext = :(call(
        Rembus.name2proc("cid", true, true),
        Rembus.CastCall(t, []),
        timeout=Rembus.request_timeout()
    ))
    fn = string(topic.args[1])
    ext.args[2].args[2] = cid
    ext.args[3].args[2] = fn

    args = topic.args[2:end]
    ext.args[3].args[3].args = args
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

macro rpc(cid, topic)
    ext = rpc_expr(topic, cid)
    quote
        $(esc(ext))
    end
end

fnname(fn::Expr) = fn.args[1].args[1]
fnname(fn::Symbol) = fn

function expose_expr(fn, cid=nothing)
    ex = :(call(
        Rembus.name2proc("cid", true, true),
        Rembus.AddImpl(aaa),
        timeout=Rembus.request_timeout()
    ))
    ex.args[3].args[2] = fnname(fn)
    ex.args[2].args[2] = cid
    return ex
end

function subscribe_expr(fn, mode::Symbol, cid=nothing)
    if mode == :from_now
        sts = false
    elseif mode == :before_now
        sts = true
    else
        return :(throw(
            ErrorException("subscribe invalid mode: must be from_now or before_now")
        ))
    end
    ex = :(call(
        Rembus.name2proc(cid, true, true),
        Rembus.AddInterest(aaa, $sts),
        timeout=Rembus.request_timeout()
    ))
    ex.args[3].args[2] = fnname(fn)
    ex.args[2].args[2] = cid
    return ex
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

macro expose(cid, fn::Symbol)
    ex = expose_expr(fn, cid)
    quote
        $(esc(ex))
        nothing
    end
end

macro expose(cid, fn::Expr)
    ex = expose_expr(fn, cid)
    quote
        $(esc(fn))
        $(esc(ex))
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
macro subscribe(fn::Symbol, mode::Symbol=:from_now)
    quote
        $(esc(subscribe_expr(fn, mode)))
        nothing
    end
end

"""
    @subscribe function fn(args...)
        ...
    end [mode]

Subscribe the function expression.
"""
macro subscribe(fn::Expr, mode::Symbol=:from_now)
    ex = subscribe_expr(fn, mode)
    quote
        $(esc(fn))
        $(esc(ex))
        nothing
    end
end

macro subscribe(cid, fn::Expr, mode::Symbol=:from_now)
    ex = subscribe_expr(fn, mode, cid)
    quote
        $(esc(fn))
        $(esc(ex))
        nothing
    end
end

macro subscribe(cid, fn::Symbol, mode::Symbol=:from_now)
    ex = subscribe_expr(fn, mode, cid)
    quote
        $(esc(ex))
        nothing
    end
end

"""
    @unexpose fn

The methods of `fn` function is no more available to rpc clients.
"""
macro unexpose(fn::Symbol)
    :(@unexpose getcomponent() $(esc(fn)))
end

macro unexpose(cid, fn)
    ex = :(call(
        Rembus.name2proc("cid"),
        Rembus.RemoveImpl(aaa),
        timeout=Rembus.request_timeout()
    ))
    ex.args[3].args[2] = fn
    ex.args[2].args[2] = cid
    quote
        $(esc(ex))
        nothing
    end
end

"""
    @unsubscribe mytopic

The methods of `mytopic` function stop to handle messages
published to topic `mytopic`.
"""
macro unsubscribe(fn::Symbol)
    :(@unsubscribe getcomponent() $(esc(fn)))
end

macro unsubscribe(cid, fn)
    ex = :(call(
        Rembus.name2proc("cid"),
        Rembus.RemoveInterest(aaa),
        timeout=Rembus.request_timeout()
    ))
    ex.args[3].args[2] = fn
    ex.args[2].args[2] = cid
    quote
        $(esc(ex))
        nothing
    end
end

function reactive_expr(reactive, cid=nothing)
    ex = :(call(
        Rembus.name2proc("cid"),
        Rembus.Reactive($reactive),
        timeout=Rembus.request_timeout()
    ))
    ex.args[2].args[2] = cid
    return ex
end

function enable_ack_expr(enable, cid=nothing)
    ex = :(call(
        Rembus.name2proc("cid"),
        Rembus.EnableAck($enable),
        timeout=Rembus.request_timeout()
    ))
    ex.args[2].args[2] = cid
    return ex
end

"""
    @reactive

The subscribed methods start to handle published messages.
"""
macro reactive(cid=nothing)
    ex = reactive_expr(true, cid)
    quote
        $(esc(ex))
        nothing
    end
end

"""
    @unreactive

The subscribed methods stop to handle published messages.
"""
macro unreactive(cid=nothing)
    ex = reactive_expr(false, cid)
    quote
        $(esc(ex))
        nothing
    end
end

#=
    @enable_ack

Enable acknowledge receipt of published messages.

This feature assure that messages get delivered at least one time to the
subscribed component.

For default the acknowledge is disabled.
=#
macro enable_ack(cid=nothing)
    ex = enable_ack_expr(true, cid)
    quote
        $(esc(ex))
        nothing
    end
end

#=
    @disable_ack

Disable acknowledge receipt of published messages.

This feature assure that messages get delivered at least one to the
subscribed component.
=#
macro disable_ack(cid=nothing)
    ex = enable_ack_expr(false, cid)
    quote
        $(esc(ex))
        nothing
    end
end

struct SetHolder
    shared::Any
end

struct AddImpl
    fn::Function
end

struct RemoveImpl
    fn::Function
end

struct AddInterest
    fn::Function
    retroactive::Bool
end

struct RemoveInterest
    fn::Function
end

struct Reactive
    status::Bool
end
struct EnableAck
    status::Bool
end

function provide(server::Embedded, name::AbstractString, func::Function)
    server.topic_function[name] = func
end

provide(server::Embedded, func::Function) = provide(server, string(func), func)


"""
    shared(rb::RBHandle, ctx)

Bind a `ctx` context object to the `rb` component.

When a `ctx` context object is bound then it will be the first argument of subscribed and
exposed methods.

See [`@shared`](@ref) for more details.
"""
shared(rb::RBHandle, ctx) = rb.shared = ctx

function rembus(cid=nothing)
    if cid === nothing
        id = Rembus.CONFIG.cid
    else
        id = cid
    end

    cmp = Component(id)
    rb = RBConnection(cmp)
    process(
        cmp.id,
        rembus_task,
        args=(rb, cmp.protocol),
        debounce_time=CONFIG.connection_retry_period,
        force_interrupt_after=3.0)
end

mutable struct LastErrorLog
    msg::Union{Nothing,String}
    LastErrorLog() = new(nothing)
end

const last_error = LastErrorLog()

function rembus_task(pd, rb, protocol=:ws)
    try
        @debug "starting rembus process: $pd, protocol:$protocol"

        connect(pd, rb)
        if last_error.msg !== nothing
            @info "[$pd] reconnected"
            last_error.msg = nothing
        end

        for msg in pd.inbox
            @debug "[$pd] recv: $msg"
            if isshutdown(msg)
                return
            elseif isa(msg, Exception)
                throw(msg)
            elseif isrequest(msg)
                req = msg.request
                if isa(req, SetHolder)
                    result = shared(rb, msg.request.shared)
                elseif isa(req, AddImpl)
                    result = expose(
                        rb, string(msg.request.fn), msg.request.fn, exceptionerror=false
                    )
                elseif isa(req, RemoveImpl)
                    result = unexpose(rb, string(msg.request.fn), exceptionerror=false)
                elseif isa(req, AddInterest)
                    result = subscribe(
                        rb,
                        string(msg.request.fn),
                        msg.request.fn,
                        msg.request.retroactive,
                        exceptionerror=false
                    )
                elseif isa(req, RemoveInterest)
                    result = unsubscribe(rb, string(msg.request.fn), exceptionerror=false)
                elseif isa(req, EnableAck)
                    if req.status
                        result = enable_ack(rb, exceptionerror=false)
                    else
                        result = disable_ack(rb, exceptionerror=false)
                    end
                elseif isa(req, Reactive)
                    if req.status
                        result = reactive(rb, exceptionerror=false)
                    else
                        result = unreactive(rb, exceptionerror=false)
                    end
                else
                    result = rpc(
                        rb, msg.request.topic, msg.request.data, exceptionerror=false
                    )
                end
                reply(msg, result)
            else
                publish(rb, msg.topic, msg.data)
            end
        end
    catch e
        if isa(e, AlreadyConnected)
            @error "[$(e.cid)] already connected"
            Rembus.CONFIG.cid = "rembus"
            return
        end

        if isa(e, HTTP.Exceptions.ConnectError)
            msg = "[$pd]: $(e.url) connection error"
        else
            msg = "[$pd] component: $e"
        end

        if last_error.msg !== msg
            @error msg
            last_error.msg = msg
        end

        @showerror e

        if isa(e, HTTP.Exceptions.ConnectError) &&
           isa(e.error.ex, HTTP.OpenSSL.OpenSSLError)
            @info "unrecoverable error $(e.error.ex): stop connection retry"
        else
            rethrow()
        end
    finally
        @debug "[$pd]: terminating"
        close(rb)
    end
end

mutable struct NullProcess <: Visor.Supervised
    id::String
    inbox::Channel
    NullProcess(id) = new(id, Channel(1))
end

add_receiver(ctx, method_name, impl) = ctx.receiver[method_name] = impl

remove_receiver(ctx, method_name) = delete!(ctx.receiver, method_name)

#=
function when_connected(fn, rb)
    while !isconnected(rb)
        sleep(1)
    end
    fn()
end
=#

#=
    invoke(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `topic` name.
=#
function invoke(rb::RBConnection, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, rb.receiver[topic](msg.data...)
        else
            return STS_SUCCESS, rb.receiver[topic](rb.shared, msg.data...)
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, rb.receiver[topic](msg.data)
        else
            return STS_SUCCESS, rb.receiver[topic](rb.shared, msg.data)
        end
    end
end

#=
    invoke_latest(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `topic` name using `Base.invokelatest`.
=#
function invoke_latest(rb::RBConnection, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, Base.invokelatest(rb.receiver[topic], msg.data...)
        else
            return (
                STS_SUCCESS, Base.invokelatest(rb.receiver[topic], rb.shared, msg.data...)
            )
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, Base.invokelatest(rb.receiver[topic], msg.data)
        else
            return STS_SUCCESS, Base.invokelatest(rb.receiver[topic], rb.shared, msg.data)
        end
    end
end

#=
    invoke_glob(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `*` name for received messages with any topic.
=#
function invoke_glob(rb::RBConnection, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, rb.receiver["*"](msg.topic, msg.data...)
        else
            return STS_SUCCESS, rb.receiver["*"](rb.shared, msg.topic, msg.data...)
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, rb.receiver["*"](msg.topic, msg.data)
        else
            return STS_SUCCESS, rb.receiver["*"](rb.shared, msg.topic, msg.data)
        end
    end
end

function rembus_handler(rb, msg, receiver)
    fn::String = msg.topic
    if haskey(rb.receiver, fn)
        try
            return receiver(rb, fn, msg)
        catch e
            @showerror e
            return STS_METHOD_EXCEPTION, string(e)
        end
    elseif haskey(rb.receiver, "*")
        try
            invoke_glob(rb, msg)
        catch e
            @error "glob subscriber: $e"
        finally
            return STS_SUCCESS, nothing
        end
    else
        return STS_METHOD_NOT_FOUND, "method $fn not found"
    end
end

function handle_input(rb, msg)
    #@debug "<< [$(rb.client.id)] <- $msg"

    if isresponse(msg)
        if haskey(rb.out, msg.id)
            # prevent requests timeouts because when jit compiling
            # notify() may be called before wait()
            yield()
            lock(rb.out[msg.id]) do
                notify(rb.out[msg.id], msg)
            end
            #while notify(rb.out[msg.id], msg) == 0
            #    sleep(0.001)
            #end
        else
            # it is a response without a waiting Condition
            if msg.status == STS_CHALLENGE
                @async resend_attestate(rb, msg)
            else
                @warn "ignoring response: $msg"
            end
        end
    else
        if isinteractive()
            sts, result = rembus_handler(rb, msg, invoke_latest)
        else
            sts, result = rembus_handler(rb, msg, invoke)
        end

        if sts === STS_METHOD_EXCEPTION
            @warn "[$(msg.topic)] method error: $result"
        end
        if isa(msg, RpcReqMsg)
            response = ResMsg(msg.id, sts, result)
            @debug "response: $response"
            put!(rb.msgch, response)
        elseif isa(msg, PubSubMsg) && (msg.flags & ACK_FLAG) == ACK_FLAG
            # check if ack is enabled
            # @debug "$msg sending Ack with hash=$(msg.id)"
            put!(rb.msgch, AckMsg(msg.id))
        end
    end

    return nothing
end

#=
    parse_msg(rb, response)

Handle a received message.
=#
function parse_msg(rb, response)
    try
        msg = connected_socket_load(response)
        handle_input(rb, msg)
    catch e
        @error "message decoding: $e"
        @showerror e
    end

    return nothing
end

keep_alive(socket::TCPSocket) = nothing

function keep_alive(socket::WebSockets.WebSocket)
    CONFIG.ws_ping_interval == 0 && return

    while true
        sleep(CONFIG.ws_ping_interval)
        if isopen(socket.io)
            @debug "socket ping"
            ping(socket)
        else
            @debug "socket connection closed, keep alive done"
            break
        end
    end
end

processput!(process::NullProcess, e) = nothing
processput!(process::Visor.Process, e) = put!(process.inbox, e)

function read_socket(socket, process, rb, isconnected::Condition)
    try
        rb.socket = socket
        yield()
        # signal to the initiator function _connect that the connection is up.
        notify(isconnected)
        #while notify(isconnected) == 0
        #    sleep(0.001)
        #end

        @async keep_alive(rb.socket)
        while isopen(socket)
            response = transport_read(socket)
            if !isempty(response)
                parse_msg(rb, response)
            else
                @debug "[$(rb.client.id)] connection closed"
                close(socket)
            end
        end
    catch e
        @debug "[$(rb.client.id)] connection closed: $e"
        if !isa(e, HTTP.WebSockets.WebSocketError) ||
           !isa(e.message, HTTP.WebSockets.CloseFrameBody) ||
           e.message.status != 1000
            @showerror e
            processput!(process, e)
        end
    end
end

function write_task(rb::RBConnection)

    try
        for msg in rb.msgch
            if isa(msg, CloseConnection)
                if rb.socket !== nothing
                    if isa(rb.socket, ZMQ.Socket)
                        transport_send(rb, rb.socket, Close())
                        close(rb.context)
                    else
                        close(rb.socket)
                    end
                end

                break
            else
                rembus_write(rb, msg)
            end
        end
    catch e
        @error "write_task: $e"
    finally
        rb.socket = nothing
        close(rb.msgch)
    end
    @debug "[$(rb.client.id)] write_task done"
end

function ws_connect(rb, process, isconnected::Condition)
    try
        url = brokerurl(rb.client)
        uri = URI(url)

        if uri.scheme == "wss"

            if !haskey(ENV, "HTTP_CA_BUNDLE")
                ENV["HTTP_CA_BUNDLE"] = joinpath(rembus_dir(), "ca", REMBUS_CA)
            end
            @debug "cacert: $(ENV["HTTP_CA_BUNDLE"])"
            HTTP.WebSockets.open(socket -> begin
                    read_socket(socket, process, rb, isconnected)
                end, url)
        elseif uri.scheme == "ws"
            HTTP.WebSockets.open(socket -> begin
                    ## Sockets.nagle(socket.io.io, false)
                    ## Sockets.quickack(socket.io.io, true)
                    read_socket(socket, process, rb, isconnected)
                end, url, idle_timeout=1, forcenew=true)
        else
            error("ws endpoint: wrong $(uri.scheme) scheme")
        end
    catch e
        notify(isconnected, e, error=true)
        @showerror e
    end
end

function zmq_receive(rb)
    while true
        try
            msg = zmq_load(rb.socket)
            handle_input(rb, msg)
        catch e
            if !isopen(rb.socket)
                break
            else
                @error "zmq message decoding: $e"
                @showerror e
            end
        end
    end
    @debug "zmq socket closed"
end

function zmq_connect(rb)
    rb.context = ZMQ.Context()
    rb.socket = ZMQ.Socket(rb.context, DEALER)
    rb.socket.linger = 1
    url = brokerurl(rb.client)
    ZMQ.connect(rb.socket, url)
    @async zmq_receive(rb)
    return nothing
end

function tcp_connect(rb, process, isconnected::Condition)
    try
        url = brokerurl(rb.client)
        uri = URI(url)
        cacert = get(ENV, "HTTP_CA_BUNDLE", joinpath(rembus_dir(), "ca", REMBUS_CA))
        @debug "connecting to $(uri.scheme):$(uri.host):$(uri.port)"
        if uri.scheme == "tls"
            entropy = MbedTLS.Entropy()
            rng = MbedTLS.CtrDrbg()
            MbedTLS.seed!(rng, entropy)

            ctx = MbedTLS.SSLContext()

            sslconf = MbedTLS.SSLConfig(true)
            MbedTLS.config_defaults!(sslconf)

            MbedTLS.rng!(sslconf, rng)

            MbedTLS.ca_chain!(sslconf, MbedTLS.crt_parse(read(cacert, String)))

            function show_debug(level, filename, number, msg)
                println((level, filename, number, msg))
            end

            MbedTLS.dbg!(sslconf, show_debug)

            sock = Sockets.connect(uri.host, parse(Int, uri.port))
            MbedTLS.setup!(ctx, sslconf)
            MbedTLS.set_bio!(ctx, sock)
            MbedTLS.handshake(ctx)

            @async read_socket(ctx, process, rb, isconnected)
        elseif uri.scheme == "tcp"
            sock = Sockets.connect(uri.host, parse(Int, uri.port))
            @async read_socket(sock, process, rb, isconnected)
        else
            error("tcp endpoint: wrong $(uri.scheme) scheme")
        end
    catch e
        notify(isconnected, e, error=true)
    end
end

function pkfile(name)
    cfgdir = rembus_dir()
    if !isdir(cfgdir)
        mkpath(cfgdir)
    end

    return joinpath(cfgdir, name)
end

function resend_attestate(rb, response)
    try
        msg = attestate(rb, response)
        put!(rb.msgch, msg)
        if rb.client.protocol == :zmq
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> ping(rb), CONFIG.zmq_ping_interval)
        end
    catch e
        @error "resend_attestate: $e"
        @showerror e
    end

    return nothing
end

function attestate(rb, response)
    file = pkfile(rb.client.id)
    if !isfile(file)
        error("unable to find $(rb.client.id) secret")
    end

    try
        ctx = MbedTLS.parse_keyfile(file)
        plain = encode([Vector{UInt8}(response.data), rb.client.id])
        hash = MbedTLS.digest(MD_SHA256, plain)
        signature = MbedTLS.sign(ctx, MD_SHA256, hash, MersenneTwister(0))
        return Attestation(rb.client.id, signature)
    catch e
        if isa(e, MbedTLS.MbedException)
            # try with a plain secret
            secret = readline(file)
            plain = encode([Vector{UInt8}(response.data), secret])
            hash = MbedTLS.digest(MD_SHA256, plain)
            @debug "[$(rb.client.id)] digest: $hash"
            return Attestation(rb.client.id, hash)
        end
    end
end

function authenticate(rb)
    if rb.client.id == "rembus"
        return nothing
    end

    reason = nothing
    msg = IdentityMsg(rb.client.id)
    response = response_or_timeout(rb, msg, request_timeout())
    if (response.status == STS_GENERIC_ERROR)
        close(rb.socket)
        throw(AlreadyConnected(rb.client.id))
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(rb, response)
        response = response_or_timeout(rb, msg, request_timeout())
    end

    if (response.status != STS_SUCCESS)
        close(rb.socket)
        rembuserror(code=response.status, reason=reason)
    else
        if rb.client.protocol == :zmq
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> ping(rb), CONFIG.zmq_ping_interval)
        end
    end
    return nothing
end

function connect_timeout(rb, isconnected)
    @debug "[$(rb.client.id)] socket: $(rb.socket)"
    if rb.socket === nothing
        notify(isconnected, ErrorException("connection failed"), error=true)
    end
end

function _connect(rb, process)
    # manage reconnections events
    if !isopen(rb.msgch)
        rb.msgch = Channel(MESSAGE_CHANNEL_SZ)
    end

    if rb.client.protocol === :ws || rb.client.protocol === :wss
        isconnected = Condition()
        t = Timer((tim) -> connect_timeout(rb, isconnected), connect_request_timeout())
        @async ws_connect(rb, process, isconnected)
        wait(isconnected)
        close(t)
    elseif rb.client.protocol === :tcp || rb.client.protocol === :tls
        isconnected = Condition()
        @async tcp_connect(rb, process, isconnected)
        wait(isconnected)
    elseif rb.client.protocol === :zmq
        zmq_connect(rb)
    else
        throw(ErrorException(
            "wrong protocol $(rb.client.protocol): must be tcp|tls|zmq|ws|wss"
        ))
    end

    @async write_task(rb)
    return rb
end

function ping(socket)
    try
        WebSockets.ping(socket)
    catch e
        @error "socket ping: $e"
    end

    return nothing
end

function rembus_write(rb::RBHandle, msg)
    @debug ">> [$(rb.client.id)] -> $msg"
    transport_send(rb, rb.socket, msg)
    return nothing
end

#=
function configure(rb::RBHandle, retroactives=Dict(), interests=Dict(), impls=Dict())
    for (topic, fn) in retroactives
        subscribe(rb, topic, fn, true)
    end
    for (topic, fn) in interests
        subscribe(rb, topic, fn, false)
    end
    for (topic, fn) in impls
        expose(rb, topic, fn)
    end

    return rb
end
=#

function isconnected(rb::RBConnection)
    if rb.socket === nothing
        return false
    else
        if isa(rb.socket, WebSockets.WebSocket)
            return isopen(rb.socket.io)
        elseif isa(rb.socket, TCPSocket)
            return !(
                rb.socket.status === Base.StatusClosed ||
                rb.socket.status === Base.StatusEOF
            )
        elseif isa(rb.socket, ZMQ.Socket)
            return isopen(rb.socket)
        else
            return !(
                rb.socket.bio.status === Base.StatusClosed ||
                rb.socket.bio.status === Base.StatusEOF
            )
        end
    end
end

isconnected(rb::RBPool) = any(c -> isconnected(c), rb.connections)

function connect(rb::RBConnection)
    if !isconnected(rb)
        _connect(rb, NullProcess(rb.client.id))
        authenticate(rb)
    end

    return rb
end

"""
    connect()

Connect anonymously to the endpoint declared with `REMBUS_BASE_URL` env variable.

`REMBUS_BASE_URL` default to `ws://127.0.0.1:8000`

A component is considered anonymous when a different and random UUID is used as
component identifier each time the application connect to the broker.
"""
function connect()
    rb = RBConnection()
    return connect(rb)
end

"""
    connect(url::AbstractString)::RBHandle

Connect to the broker.

The returned rembus handler do not auto-reconnect in case of a fault condition.

The returned `RBHandle` handle represents a connected component
used for the Rembus APIs. For example:

```julia
using Rembus
rb = connect("mycomponent")
publish(rb, "temperature", ["room_1", 21.5])
```

The `url` argument string is formatted as:

`url = [<protocol>://][<host>][:<port>/]<cid>`

`<protocol>` is one of:

- `ws` web socket
- `wss` secure web socket
- `tcp` tcp socket
- `tls` TLS over tcp socket
- `zmq` ZeroMQ socket

`<host>` and `<port>` are the hostname/ip and the port of the listening broker.

`<cid>` is the unique name of the component.
"""
function connect(url::AbstractString)::RBHandle
    process = NullProcess(url)
    rb = RBConnection(process.id)
    return connect(rb)
end

#=
    connect(process::Visor.Supervised, rb::RBHandle)

Connect the component defined by the `rb` handle to the broker.

The supervised task `process` receives an `Exception` message when
an exception is thrown by the `read_socket()`.

The `process` supervisor try to auto-reconnect if an exception occurs.
=#
function connect(process::Visor.Supervised, rb::RBHandle)
    _connect(rb, process)
    authenticate(rb)

    # register again callbacks
    for (name, fn) in rb.receiver
        if haskey(rb.subinfo, name)
            subscribe(rb, name, fn, rb.subinfo[name], exceptionerror=false)
        else
            expose(rb, name, fn, exceptionerror=false)
        end
    end

    if rb.reactive
        reactive(rb)
    end

    return rb
end

function connect(rb::RBPool)
    for c in rb.connections
        try
            connect(c)
        catch e
            @warn "[$(c.client.id)] error: $e"
        end
    end

    return rb
end

function connect(urls::Vector)
    pool = RBPool([RBConnection(url) for url in urls])
    return connect(pool)
end

#=
function login(rb::RBHandle, cid::AbstractString, secret::AbstractString)
    try
        challenge = rpc(rb, "challenge")
        attestation = MbedTLS.digest(MbedTLS.MD_SHA256, encode([challenge, secret]))
        @debug "[$cid] digest: $attestation"
        rpc(rb, "login", [cid, attestation]) || error("invalid password")
    catch e
        error("login failed: $e")
    end

    return nothing
end
=#

function Base.close(rb::RBPool)
    for c in rb.connections
        close(c)
    end
end

function Base.close(rb::RBConnection)
    if rb.socket !== nothing
        # connection is established
        put!(rb.msgch, CloseConnection())
        while (isopen(rb.msgch))
            sleep(0.05)
        end
    end
    return nothing
end

#=
function assert_rembus(process::Visor.Process)
    if length(process.args) == 0 || !isa(process.args[1], RBHandle)
        throw(ErrorException("invalid $process process: not a rembus process"))
    end
end
=#

function enable_debug(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => ENABLE_DEBUG_CMD)),
        exceptionerror=exceptionerror
    )
end

function disable_debug(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => DISABLE_DEBUG_CMD)),
        exceptionerror=exceptionerror
    )
end

function broker_config(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => BROKER_CONFIG_CMD)),
        exceptionerror=exceptionerror
    )
end

function private_topics_config(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => PRIVATE_TOPICS_CONFIG_CMD)),
        exceptionerror=exceptionerror
    )
end

function load_config(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => LOAD_CONFIG_CMD)),
        exceptionerror=exceptionerror
    )
end

function save_config(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => SAVE_CONFIG_CMD)),
        exceptionerror=exceptionerror
    )
end

function disable_ack(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => DISABLE_ACK_CMD)),
        exceptionerror=exceptionerror
    )
end

function enable_ack(rb::RBHandle; exceptionerror=true)
    return rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => ENABLE_ACK_CMD)),
        exceptionerror=exceptionerror
    )
end

"""
    unreactive(rb::RBHandle, timeout=5; exceptionerror=true)

Stop the delivery of published message.
"""
function unreactive(rb::RBHandle; exceptionerror=true)
    response = rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => REACTIVE_CMD, STATUS => false)),
        exceptionerror=exceptionerror
    )
    rb.reactive = false

    return response
end

"""
    reactive(rb::RBHandle, timeout=5; exceptionerror=true)

Start the delivery of published messages for which there was declared
an interest with [`subscribe`](@ref).
"""
function reactive(rb::RBHandle; exceptionerror=true)
    response = rpcreq(
        rb,
        AdminReqMsg(BROKER_CONFIG, Dict(COMMAND => REACTIVE_CMD, STATUS => true)),
        exceptionerror=exceptionerror
    )
    rb.reactive = true

    return response
end

"""
    subscribe(rb::RBHandle, fn::Function, retroactive::Bool=false; exceptionerror=true)
    subscribe(
        rb::RBHandle, topic::AbstractString, fn::Function, retroactive::Bool=false;
        exceptionerror=true
    )

Declare interest for messages published on `topic` logical channel.

The function `fn` is called when a message is received on `topic` and
[`reactive`](@ref) put the `rb` component in reactive mode.

If the `topic` argument is omitted the function name must be equal to the topic name.

If `retroactive` is `true` then `rb` component will receive messages published when it was
offline.
"""
function subscribe(
    rb::RBHandle, topic::AbstractString, fn::Function, retroactive::Bool=false;
    exceptionerror=true
)
    add_receiver(rb, topic, fn)
    rb.subinfo[topic] = retroactive
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => SUBSCRIBE_CMD, RETROACTIVE => retroactive)),
        exceptionerror=exceptionerror
    )
end

function subscribe(
    rb::RBHandle, fn::Function, retroactive::Bool=false;
    exceptionerror=true
)
    return subscribe(rb, string(fn), fn, retroactive; exceptionerror=exceptionerror)
end

"""
    unsubscribe(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    unsubscribe(rb::RBHandle, fn::Function; exceptionerror=true)

No more messages published on a `topic` logical channel or a topic name equals to the name
of the subscribed function will be delivered to `rb` component.
"""
function unsubscribe(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    remove_receiver(rb, topic)
    delete!(rb.subinfo, topic)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => UNSUBSCRIBE_CMD)),
        exceptionerror=exceptionerror
    )
end

function unsubscribe(rb::RBHandle, fn::Function; exceptionerror=true)
    return unsubscribe(rb, string(fn); exceptionerror=exceptionerror)
end

"""
    expose(rb::RBHandle, fn::Function; exceptionerror=true)
    expose(rb::RBHandle, topic::AbstractString, fn::Function; exceptionerror=true)

Expose the methods of function `fn` to be executed by rpc clients using `topic` as
RPC method name.

If the `topic` argument is omitted the function name equals to the RPC method name.

`fn` returns the RPC response.
"""
function expose(rb::RBHandle, topic::AbstractString, fn::Function; exceptionerror=true)
    add_receiver(rb, topic, fn)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => EXPOSE_CMD)),
        exceptionerror=exceptionerror
    )
end

function expose(rb::RBHandle, fn::Function; exceptionerror=true)
    return expose(rb, string(fn), fn; exceptionerror=exceptionerror)
end

"""
    unexpose(rb::RBHandle, fn::Function; exceptionerror=true)
    unexpose(rb::RBHandle, topic::AbstractString; exceptionerror=true)

Stop servicing RPC requests targeting `topic` or `fn` methods.
"""
function unexpose(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    remove_receiver(rb, topic)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => UNEXPOSE_CMD)),
        exceptionerror=exceptionerror
    )
end

function unexpose(rb::RBHandle, fn::Function; exceptionerror=true)
    return unexpose(rb, string(fn), exceptionerror=exceptionerror)
end

function private_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => PRIVATE_TOPIC_CMD)),
        exceptionerror=exceptionerror
    )
end

function public_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => PUBLIC_TOPIC_CMD)),
        exceptionerror=exceptionerror
    )
end

function authorize(
    rb::RBHandle, client::AbstractString, topic::AbstractString;
    exceptionerror=true
)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => AUTHORIZE_CMD, CID => client)),
        exceptionerror=exceptionerror
    )
end

function unauthorize(
    rb::RBHandle, client::AbstractString, topic::AbstractString;
    exceptionerror=true
)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => UNAUTHORIZE_CMD, CID => client)),
        exceptionerror=exceptionerror
    )
end

# """
#     ping(rb::RBHandle)
#
# Send a ping message to check if the broker is online.
#
# Required by ZeroMQ socket.
# """
function ping(rb::RBConnection)
    try
        if rb.client.protocol == :zmq
            if isconnected(rb)
                rpcreq(rb, PingMsg(rb.client.id))
            end
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> ping(rb), CONFIG.zmq_ping_interval)
        end
    catch e
        @debug "[$(rb.client.id)]: pong not received"
        @showerror e
    end

    return nothing
end

"""
    publish(rb::RBHandle, topic::AbstractString, data=[])

Publish `data` values on `topic`.

`data` may be a value or a vector of values. Each value map to the arguments of the
subscribed method.

For example if the subscriber is a method that expects two arguments:

```
mytopic(x,y) = @info "x=\$x, y=\$y"
```

The published message needs an array of two elements:

```
publish(rb, "mytopic", [1, 2])
```

When a subscribed method expect one argument instead of passing an array of one element
it may be better to pass the value:

```
mytopic(x) = @info "x=\$x"

publish(rb, "mytopic", 1)
```

If the subscribed method has no arguments invoke `publish` as:

```
mytopic() = @info "mytopic invoked"

publish(rb, "mytopic")
```

`data` array may contains any type, but if the components are implemented in different
languages then data has to be a DataFrame or a primitive type that is
[CBOR](https://www.rfc-editor.org/rfc/rfc8949.html) encodable.
"""
function publish(rb::RBHandle, topic::AbstractString, data=[])
    put!(rb.msgch, PubSubMsg(topic, data))
    return nothing
end

function publish_ack(rb::RBHandle, topic::AbstractString, data=[])
    put!(rb.msgch, PubSubMsg(topic, data, ACK_FLAG))
    return nothing
end


"""
    rpc(rb::RBHandle,
        topic::AbstractString,
        data=nothing;
        exceptionerror=true,
        timeout=request_timeout())

Call the remote `topic` method with arguments extracted from `data`.

## Exposer

```julia
using Rembus
using Statistics

@expose service_noargs() = "success"

@expose service_name(name) = "hello " * name

@expose service_dictionary(d) = mean(values(d))

@expose function service_multiple_args(name, score, flags)
    isa(name, String) && isa(score, Float64) && isa(flags, Vector)
end
```

## RPC client

```julia
using Rembus

rb = connect()

rcp(rb, "service_noargs")

rpc(rb, "service_name", "hello world")

rpc(rb, "service_dictionary", Dict("r1"=>13.3, "r2"=>3.0))

rpc(rb, "service_multiple_args", ["name", 1.0, ["red"=>1,"blue"=>2,"yellow"=>3]])
```
"""
function rpc(rb::RBHandle, topic::AbstractString, data=[];
    exceptionerror=true, timeout=request_timeout())
    rpcreq(rb, RpcReqMsg(topic, data), exceptionerror=exceptionerror, timeout=timeout)
end

function direct(
    rb::RBHandle, target::AbstractString, topic::AbstractString, data=nothing;
    exceptionerror=true
)
    return rpcreq(rb, RpcReqMsg(topic, data, target), exceptionerror=exceptionerror)
end

function response_timeout(condition::Threads.Condition, msg::RembusMsg)
    if hasproperty(msg, :topic)
        descr = "[$(msg.topic)]: request timeout"
    else
        descr = "[$msg]: request timeout"
    end

    lock(condition) do
        notify(condition, RembusTimeout(descr), error=false)
    end

    return nothing
end

function response_or_timeout(rb::RBHandle, msg::RembusMsg, timeout)
    response = wait_response(rb, msg, request_timeout())
    if isa(response, RembusTimeout)
        close(rb.socket)
        throw(response)
    end

    return response
end

# https://github.com/JuliaLang/julia/issues/36217
function wait_response(rb::RBHandle, msg::RembusMsg, timeout)
    mid::UInt128 = msg.id
    resp_cond = Threads.Condition()
    rb.out[mid] = resp_cond
    t = Timer((tim) -> response_timeout(resp_cond, msg), timeout)
    put!(rb.msgch, msg)
    lock(resp_cond)
    res = wait(resp_cond)
    unlock(resp_cond)
    close(t)
    delete!(rb.out, mid)
    return res
end

# Send a RpcReqMsg message to rembus and return the response.
function rpcreq(handle::RBHandle, msg; exceptionerror=true, timeout=request_timeout())
    outcome = nothing
    !isconnected(handle) && error("connection is down")

    if isa(handle, RBPool)
        if CONFIG.balancer === "first_up"
            rb = first_up(handle, msg.topic, handle.connections)
        elseif CONFIG.balancer === "round_robin"
            rb = round_robin(handle, msg.topic, handle.connections)
        else
            rb = less_busy(handle, msg.topic, handle.connections)
        end
    else
        rb = handle
    end

    response = wait_response(rb, msg, timeout)
    if isa(response, RembusTimeout)
        outcome = response
        if exceptionerror
            throw(outcome)
        end
    elseif response.status == STS_SUCCESS
        outcome = response.data
    elseif response.status == STS_CHALLENGE
        @async resend_attestate(rb, response)
    elseif (response.status == STS_SHUTDOWN)
        outcome = "shutting down"
    else
        topic = nothing
        if isa(msg, RembusTopicMsg)
            topic = msg.topic
        end
        outcome = rembuserror(
            exceptionerror,
            code=response.status,
            cid=rb.client.id,
            topic=topic,
            reason=response.data)
    end
    return outcome
end

function broker_shutdown(admin::RBConnection)
    rpcreq(admin, AdminReqMsg("__config__", Dict(COMMAND => SHUTDOWN_CMD)))
end

function waiter(pd)
    # the only message may be a shutdown request
    take!(pd.inbox)
    @info "forever done"
end

"""
    forever(rb::RBHandle)

    Start the event loop awaiting to execute exposed and subscribed methods.
"""
function forever(rb::RBHandle)
    reactive(rb)
    supervise([process(waiter)], wait=!isinteractive())
end

"""
    @forever

Start the event loop awaiting to execute exposed and subscribed methods.
"""
macro forever()
    quote
        component = getcomponent()
        process = from(component.id)
        if process !== nothing
            cmp = process.args[1]
            reactive(cmp)
            # Don't block the REPL!
            isinteractive() ? nothing : supervise()
        end
    end
end

@setup_workload begin
    ENV["REMBUS_ZMQ_PING_INTERVAL"] = "0"
    ENV["REMBUS_WS_PING_INTERVAL"] = "0"
    ENV["REMBUS_TIMEOUT"] = "20"
    ENV["REMBUS_CONNECT_TIMEOUT"] = "20"
    @compile_workload begin
        sv = Rembus.caronte(
            wait=false,
            args=Dict("ws" => 8000, "tcp" => 8001, "zmq" => 8002, "http" => 9000)
        )
        yield()
        Rembus.islistening(20)
        include("precompile.jl")
        shutdown()
    end
end

end # module
