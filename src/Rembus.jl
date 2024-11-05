#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#
module Rembus

import Distributed

using ArgParse
using Arrow
using Base64
using DocStringExtensions
using DataFrames
using Dates
using DataStructures
using DuckDB
using FileWatching
using HTTP
using JSON3
using JSONTables
using Logging
using MbedTLS
using Random
using Reexport
using Sockets
using Parameters
using Parquet
using PrecompileTools
using Preferences
using Printf
using URIs
using Serialization
using UUIDs
@reexport using Visor
using ZMQ

export @component
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
export component
export connect
export isauthenticated
export server
export expose, unexpose
export subscribe, unsubscribe
export direct
export rpc
export rpc_future
export fetch_response
export publish
export reactive, unreactive
export authorize, unauthorize
export private_topic, public_topic
export provide
export close
export isconnected
export rembus
export shared
export forever
export terminate
export egress_interceptor, ingress_interceptor
export rbinfo
export register, unregister
export anonymous!, named!, authenticated!

# broker api
export add_server, remove_server
export broker, session, republish, msg_payload

export RembusError
export RembusTimeout
export RpcMethodNotFound, RpcMethodUnavailable, RpcMethodLoopback, RpcMethodException
export SmallInteger
export QOS0, QOS1, QOS2
export LastReceived, Now

# The permitted mode of connection.
# if mode is authenticated then anonymous modes are not permitted.
# if mode is anonymous then authenticated mode is available.
@enum ConnectionMode anonymous authenticated

@enum NodeType socket zdealer zrouter loopback

struct IdentityReturn
    sts::UInt8
    value::Any
end

mutable struct Component
    id::String
    hasname::Bool
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
            name = string(uuid4())
            hasname = false
        else
            hasname = true
        end
        return new(name, hasname, protocol, host, port, props)
    end
end

Component() = Component("")

brokerurl(c::Component) = "$(c.protocol == :zmq ? :tcp : c.protocol)://$(c.host):$(c.port)"

cid(c::Component) = "$(c.protocol)://$(c.host):$(c.port)/$(c.id)"

hasname(c::Component) = c.hasname

function nodetype(c::Component)
    if c.protocol === :ws || c.protocol === :wss ||
       c.protocol === :tcp || c.protocol === :tls
        return socket
    elseif c.protocol === :zmq
        return zdealer
    end

    return loopback
end

mutable struct DBHandler
    db::DuckDB.DB
    msg_stmt::DuckDB.Stmt
end

function rembus_db(cid::AbstractString)
    db = DuckDB.DB(joinpath(rembus_dir(), "$cid.db"))
    stmts = [
        "CREATE TABLE IF NOT EXISTS received (ts UBIGINT, uid UBIGINT)"
    ]
    for stmt in stmts
        DBInterface.execute(db, stmt)
    end
    stmt = DBInterface.prepare(
        db,
        "INSERT INTO received (ts, uid) VALUES (?, ?)"
    )
    return DBHandler(db, stmt)
end

abstract type RBHandle end

mutable struct RBConnection <: RBHandle
    type::NodeType
    duck::Union{Nothing,DBHandler}
    shared::Any
    egress::Union{Nothing,Function}
    ingress::Union{Nothing,Function}
    socket::Any
    reactive::Bool
    client::Component
    receiver::Dict{String,Function}
    subinfo::Dict{String,Float64}
    out::Dict{UInt128,Distributed.Future}
    acktimer::Dict{UInt128,Timer}
    zmqcontext::Union{Nothing,ZMQ.Context}
    process::Visor.Process
    function RBConnection(name::String)
        c = Component(name)
        new(
            nodetype(c),
            nothing,
            missing,
            nothing,
            nothing,
            nothing,
            false,
            c,
            Dict(),
            Dict(),
            Dict(),
            Dict(),
            nothing
        )
    end
    RBConnection(client=getcomponent()) = new(
        nodetype(client),
        nothing,
        missing,
        nothing,
        nothing,
        nothing,
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

opstatus(rb::RBHandle) = isconnected(rb) ? '👍' : '👎'
rbinfo(rb::RBHandle) = "$(cid(rb.client))$(opstatus(rb))"
rbinfo(rb::Visor.Process) = "$rb$(opstatus(rb.args[1]))"

function Base.show(io::IO, rb::RBConnection)
    return print(io, rbinfo(rb))
end

function egress_interceptor(rb, func)
    @debug "[$rb] setting egress: $func"
    rb.egress = func
end

function ingress_interceptor(rb::RBConnection, func)
    @debug "[$rb] setting ingress: $func"
    rb.ingress = func
end

abstract type AbstractRouter end

mutable struct Server <: AbstractRouter
    shared::Any
    mode::ConnectionMode
    start_ts::Float64
    topic_function::Dict{String,Function}
    topic_auth::Dict{String,Dict{String,Bool}} # topic => {twin.id => true}
    subinfo::Dict{String,Float64}
    id_twin::Dict
    address2twin::Dict{Vector{UInt8},RBHandle} # zeromq address => twin
    twin2address::Dict{String,Vector{UInt8}} # twin id => zeromq address
    process::Visor.Supervisor
    server::Sockets.TCPServer
    http_server::HTTP.Server
    ws_server::Sockets.TCPServer
    zmqsocket::ZMQ.Socket
    zmqcontext::ZMQ.Context
    owners::DataFrame
    component_owner::DataFrame
    Server(shared=missing) = new(
        shared, anonymous, time(), Dict(), Dict(), Dict(), Dict(), Dict(), Dict()
    )
end

function Base.show(io::IO, srv::Server)
    return print(io, "$(isdefined(srv, :process) ? srv.process.id : "server")")
end

mutable struct RBServerConnection <: RBHandle
    type::NodeType
    router::Server
    client::Component
    isauth::Bool
    reactive::Bool
    session::Dict{String,Any}
    egress::Union{Nothing,Function}
    ingress::Union{Nothing,Function}
    socket::Any
    out::Dict{UInt128,Distributed.Future}
    acktimer::Dict{UInt128,Timer}
    zaddress::Vector{UInt8}
    duck::Union{Nothing,DBHandler}
    process::Visor.Process
    RBServerConnection(server::Server, name::String, type::NodeType) = new(
        type,
        server,
        Component(name),
        false,
        true, # reactive
        Dict(),
        nothing,
        nothing,
        nothing,
        Dict(),
        Dict(),
        UInt8[0, 0, 0, 0],
        nothing,
    )
end

ucid(rb::RBServerConnection) = rb.client.id

function Base.show(io::IO, rb::RBServerConnection)
    return print(io, "srv component [$(cid(rb.client))], isconnected: $(isconnected(rb))")
end

mutable struct RBPool <: RBHandle
    last_invoked::Dict{String,Int} # topic => index of last used connection
    policy::Symbol
    shared::Any
    connections::Vector{RBConnection}
    process::Visor.Process
    RBPool(
        policy=:policy_default,
        conns::Vector{RBConnection}=[]) = new(Dict(), policy, missing, conns
    )
end

cid(rb::RBHandle) = cid(rb.client)

function cid(rb::RBPool)
    cids = [cid(conn) for conn in rb.connections]
    return "pool:$(join(cids, ","))"
end

struct WsPing end

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

anonymous!() = CONFIG.connection_mode = anonymous

authenticated!() = CONFIG.connection_mode = authenticated

function __init__()
    setup(CONFIG)
    Visor.setroot(intensity=3)
    atexit(shutdown)
end

struct CloseConnection end

# Wrong tcp packet received.
struct WrongTcpPacket <: Exception
end

struct CABundleNotFound <: Exception
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
    qos::UInt8
    CastCall(topic, data, qos=QOS0) = new(topic, data, qos)
end

Base.show(io::IO, call::CastCall) = print(io, call.topic)

request_timeout() = parse(Float32, get(ENV, "REMBUS_TIMEOUT", "10"))

challenge_timeout() = parse(Float32, get(ENV, "REMBUS_CHALLENGE_TIMEOUT", "10"))

connect_request_timeout() = parse(Float32, get(ENV, "REMBUS_CONNECT_TIMEOUT", "10"))

call_timeout() = request_timeout() + 0.5

getcomponent() = Rembus.CONFIG.cid

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
        if setanonymous && !hasname(CONFIG.cid)
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
        Rembus.CONFIG.cid = Component($(esc(name)))
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
        Rembus.CONFIG.cid = Component()
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
        timeout=Rembus.call_timeout()
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

function publish_expr(topic, qos, cid=nothing)
    ext = :(cast(Rembus.name2proc("cid", true, true), Rembus.CastCall(t, [], QOS0)))
    fn = string(topic.args[1])
    ext.args[2].args[2] = cid
    ext.args[3].args[2] = fn
    args = topic.args[2:end]
    ext.args[3].args[3].args = args
    ext.args[3].args[4] = qos
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
macro publish(topic, qos::Symbol=:QOS0)
    ext = publish_expr(topic, qos)
    quote
        $(esc(ext))
    end
end

macro publish(cid, topic, qos::Symbol=:QOS0)
    ext = publish_expr(topic, qos, cid)
    quote
        $(esc(ext))
    end
end

function rpc_expr(topic, cid=nothing)
    ext = :(call(
        Rembus.name2proc("cid", true, true),
        Rembus.CastCall(t, []),
        timeout=Rembus.call_timeout()
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
        timeout=Rembus.call_timeout()
    ))
    ex.args[3].args[2] = fnname(fn)
    ex.args[2].args[2] = cid
    return ex
end

function subscribe_expr(fn, from, cid=nothing)
    ex = :(call(
        Rembus.name2proc(cid, true, true),
        Rembus.AddInterest(aaa, Now()),
        timeout=Rembus.call_timeout()
    ))
    ex.args[3].args[2] = fnname(fn)
    ex.args[3].args[3] = from.args[2]
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
macro subscribe(fn::Symbol, from=:(from = Now()))
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
macro subscribe(fn::Expr, from=:(from = Now()))
    ex = subscribe_expr(fn, from)
    quote
        $(esc(fn))
        $(esc(ex))
        nothing
    end
end

macro subscribe(cid, fn::Symbol, from)
    ex = subscribe_expr(fn, from, cid)
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
        timeout=Rembus.call_timeout()
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
        timeout=Rembus.call_timeout()
    ))
    ex.args[3].args[2] = fn
    ex.args[2].args[2] = cid
    quote
        $(esc(ex))
        nothing
    end
end

function reactive_expr(reactive, from, cid=nothing)
    ex = :(call(
        Rembus.name2proc("cid", true, true),
        Rembus.Reactive($reactive, Now()),
        timeout=Rembus.call_timeout()
    ))
    ex.args[2].args[2] = cid
    ex.args[3].args[3] = from.args[2]
    return ex
end

"""
    @reactive

The subscribed methods start to handle published messages.
"""
macro reactive(cid, from::Expr=:(from = LastReceived()))
    ex = reactive_expr(true, from, cid)
    quote
        $(esc(ex))
        nothing
    end
end

"""
    @reactive

The subscribed methods start to handle published messages.
"""
macro reactive(from::Expr=:(from = LastReceived()))
    ex = reactive_expr(true, from, nothing)
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
    ex = reactive_expr(false, :(from = NaN), cid)
    quote
        $(esc(ex))
        nothing
    end
end

struct SetHolder
    shared::Any
end

struct AddImpl
    topic::String
    fn::Function
    AddImpl(fn::Function) = new(string(fn), fn)
    AddImpl(topic::AbstractString, fn::Function) = new(topic, fn)
end

struct RemoveImpl
    fn::String
    RemoveImpl(fn::AbstractString) = new(fn)
    RemoveImpl(fn::Function) = new(string(fn))
end

struct AddInterest
    topic::String
    fn::Function
    msg_from::Union{Real,Period,Dates.CompoundPeriod}
    AddInterest(
        topic::AbstractString,
        fn::Function,
        msg_from
    ) = new(topic, fn, msg_from)
    AddInterest(
        fn::Function,
        msg_from
    ) = new(string(fn), fn, msg_from)
end

struct RemoveInterest
    fn::String
    RemoveInterest(fn::AbstractString) = new(fn)
    RemoveInterest(fn::Function) = new(string(fn))
end

struct Reactive
    status::Bool
    msg_from::Union{Real,Period,Dates.CompoundPeriod}
end

struct EnableAck
    status::Bool
end

#=
Provide an exposed server method.
=#
function expose(
    server::Server, name::AbstractString, func::Function;
    exceptionerror=true
)
    server.topic_function[name] = func
    # inform all (already) connected nodes
    for (id, twin) in server.id_twin
        rpcreq(twin,
            AdminReqMsg(name, Dict(COMMAND => EXPOSE_CMD)),
            exceptionerror=exceptionerror
        )
    end
end

expose(
    server::Server,
    func::Function;
    exceptionerror=true) = expose(server, string(func), func, exceptionerror=exceptionerror)

function subscribe(
    server::Server, name::AbstractString, func::Function; from=Now()
)
    server.topic_function[name] = func
    server.subinfo[name] = from
end

subscribe(server::Server, func::Function) = subscribe(server, string(func), func)

"""
    shared(rb::RBHandle, ctx)

Bind a `ctx` context object to the `rb` component.

When a `ctx` context object is bound then it will be the first argument of subscribed and
exposed methods.

See [`@shared`](@ref) for more details.
"""
shared(rb, ctx) = rb.shared = ctx

function rembus(cid=nothing)
    if cid === nothing
        cmp = Rembus.CONFIG.cid
    else
        cmp = Component(cid)
    end

    rb = RBConnection(cmp)
    process(
        cmp.id,
        client_task,
        args=(rb, cmp.protocol),
        debounce_time=CONFIG.connection_retry_period,
        force_interrupt_after=3.0)
end

mutable struct LastErrorLog
    msg::Union{Nothing,String}
    LastErrorLog() = new(nothing)
end

const last_error = LastErrorLog()

function call_request(rb, msg)
    req = msg.request
    if isa(req, SetHolder)
        result = shared(rb, msg.request.shared)
    elseif isa(req, AddImpl)
        result = expose(
            rb, msg.request.topic, msg.request.fn, exceptionerror=false
        )
    elseif isa(req, RemoveImpl)
        result = unexpose(rb, msg.request.fn, exceptionerror=false)
    elseif isa(req, AddInterest)
        result = subscribe(
            rb,
            msg.request.topic,
            msg.request.fn,
            from=msg.request.msg_from,
            exceptionerror=false
        )
    elseif isa(req, RemoveInterest)
        result = unsubscribe(rb, msg.request.fn, exceptionerror=false)
    elseif isa(req, Reactive)
        if req.status
            result = reactive(
                rb,
                from=msg.request.msg_from,
                exceptionerror=false
            )
        else
            result = unreactive(rb, exceptionerror=false)
        end
    else
        result = rpc(
            rb, msg.request.topic, msg.request.data, exceptionerror=false
        )
    end
    reply(msg, result)
end

function rembus_task(pd, rb, init_fn, protocol=:ws)
    try
        @debug "starting rembus process: $pd, protocol:$protocol"
        setphase(pd, :init)
        rb.process = pd
        init_fn(pd, rb)
        if last_error.msg !== nothing
            @debug "[$pd] (re)connected"
            last_error.msg = nothing
        end
        setphase(pd, :up)
        for msg in pd.inbox
            @debug "rembus_task [$pd] recv: $msg"
            if isshutdown(msg)
                return
            elseif isa(msg, Exception)
                throw(msg)
            elseif isa(msg, RembusMsg)
                send_message(rb, msg)
            elseif isrequest(msg)
                @async call_request(rb, msg)
            else
                publish(rb, msg.topic, msg.data, qos=msg.qos)
            end
        end
    catch e
        if isa(e, AlreadyConnected)
            @error "[$(e.cid)] already connected"
            Rembus.CONFIG.cid = Component("")
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

        if isa(e, CABundleNotFound)
            @info "CA bundle not found: stop connection retry"
        elseif (isa(e, HTTP.Exceptions.ConnectError) &&
                isa(e.error.ex, HTTP.OpenSSL.OpenSSLError))
            @info "unrecoverable error $(e.error.ex): stop connection retry"
        else
            rethrow()
        end
    finally
        @debug "[$pd]: terminating"
        setphase(pd, :down)
        close(rb)
    end
end

#=
The rembus process task when the connection is initiated by this component.
=#
function client_task(pd, rb, protocol=:ws)
    @debug "starting rembus process: $pd, protocol:$protocol"
    rembus_task(pd, rb, connect, protocol)
end

#=
The rembus process task related to a connection initiated by the other side:
a client or a broker.
=#
function server_task(pd, rb, protocol=:ws)
    @debug "starting rembus process: $pd, protocol:$protocol"
    rembus_task(pd, rb, bind, protocol)
end

#=
Task process that manages a pool of connections.
=#
function pool_task(pd, rb::RBPool)

    # start a process for each RBPool item
    processes = []
    for c in rb.connections
        push!(processes, component(c))
    end

    for msg in pd.inbox
        @debug "pool_task [$pd] recv: $msg"

        if isshutdown(msg)
            return
        elseif isrequest(msg)
            @async call_request(rb, msg)
            #req = msg.request
            #result = rpc(
            #    rb, req.topic, req.data, exceptionerror=false
            #)
            #reply(msg, result)
        else
            publish(rb, msg.topic, msg.data, qos=msg.qos)
        end
    end
end

mutable struct NullProcess <: Visor.Supervised
    id::String
    inbox::Channel
    NullProcess(id) = new(id, Channel(1))
end

add_receiver(rb::RBConnection, method_name, impl) = rb.receiver[method_name] = impl

add_receiver(
    rb::RBServerConnection,
    method_name,
    impl
) = rb.router.topic_function[method_name] = impl

remove_receiver(ctx, method_name) = delete!(ctx.receiver, method_name)

#=
function when_connected(fn, rb)
    while !isconnected(rb)
        sleep(1)
    end
    fn()
end
=#

get_callback(rb::RBConnection, topic) = rb.receiver[topic]

get_callback(rb::RBServerConnection, topic) = rb.router.topic_function[topic]

has_callback(rb::RBConnection, fn) = haskey(rb.receiver, fn)

has_callback(rb::RBServerConnection, fn) = haskey(rb.router.topic_function, fn)

#=
    invoke(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `topic` name.
=#
function invoke(rb::RBHandle, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, get_callback(rb, topic)(msg.data...)
        else
            return STS_SUCCESS, get_callback(rb, topic)(rb.shared, msg.data...)
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, get_callback(rb, topic)(msg.data)
        else
            return STS_SUCCESS, get_callback(rb, topic)(rb.shared, msg.data)
        end
    end
end

function invoke(rb::RBServerConnection, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.router.shared === missing
            return STS_SUCCESS, get_callback(rb, topic)(msg.data...)
        else
            return STS_SUCCESS, get_callback(rb, topic)(rb.router.shared, msg.data...)
        end
    else
        if rb.router.shared === missing
            return STS_SUCCESS, get_callback(rb, topic)(msg.data)
        else
            return STS_SUCCESS, get_callback(rb, topic)(rb.router.shared, msg.data)
        end
    end
end

#=
    invoke_latest(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `topic` name using `Base.invokelatest`.
=#
function invoke_latest(rb::RBHandle, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, Base.invokelatest(get_callback(rb, topic), msg.data...)
        else
            return (
                STS_SUCCESS, Base.invokelatest(
                    get_callback(rb, topic), rb.shared, msg.data...
                )
            )
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, Base.invokelatest(get_callback(rb, topic), msg.data)
        else
            return STS_SUCCESS, Base.invokelatest(
                get_callback(rb, topic), rb.shared, msg.data
            )
        end
    end
end

function invoke_latest(rb::RBServerConnection, topic::AbstractString, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.router.shared === missing
            return STS_SUCCESS, Base.invokelatest(get_callback(rb, topic), msg.data...)
        else
            return (
                STS_SUCCESS, Base.invokelatest(
                    get_callback(rb, topic), rb.router.shared, msg.data...
                )
            )
        end
    else
        if rb.router.shared === missing
            return STS_SUCCESS, Base.invokelatest(get_callback(rb, topic), msg.data)
        else
            return STS_SUCCESS, Base.invokelatest(
                get_callback(rb, topic), rb.router.shared, msg.data
            )
        end
    end

end

#=
    invoke_glob(rb::RBConnection, topic::AbstractString, msg::RembusMsg)

Invoke the method registered with `*` name for received messages with any topic.
=#
function invoke_glob(rb::RBHandle, msg::RembusMsg)
    if isa(msg.data, Vector)
        if rb.shared === missing
            return STS_SUCCESS, get_callback(rb, "*")(msg.topic, msg.data...)
        else
            return STS_SUCCESS, get_callback(rb, "*")(rb.shared, msg.topic, msg.data...)
        end
    else
        if rb.shared === missing
            return STS_SUCCESS, get_callback(rb, "*")(msg.topic, msg.data)
        else
            return STS_SUCCESS, get_callback(rb, "*")(rb.shared, msg.topic, msg.data)
        end
    end
end

function rembus_handler(rb::RBHandle, msg, receiver)
    fn::String = msg.topic
    if has_callback(rb, fn)
        try
            return receiver(rb, fn, msg)
        catch e
            @showerror e
            return STS_METHOD_EXCEPTION, string(e)
        end
    elseif has_callback(rb, "*")
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
    #@debug "<< [$(cid(rb.client))] <- $msg"
    if rb.ingress !== nothing
        msg = rb.ingress(rb, msg)
    end

    if msg === nothing
        return nothing
    end

    # True for AckMsg and ResMsg
    if isresponse(msg)
        if haskey(rb.out, msg.id)
            # prevent requests timeouts because when jit compiling
            # notify() may be called before wait()
            yield()
            if isa(msg, AckMsg)
                put!(rb.out[msg.id], true)
            else
                put!(rb.out[msg.id], msg)
            end
            delete!(rb.out, msg.id)
        else
            # it is a response without a waiting Condition
            if msg.id == CONNECTION_ID
                @debug "unexpected unsolicited challenge"
            elseif msg.status == STS_CHALLENGE
                @async resend_attestate(rb, msg)
            else
                @warn "ignoring response: $msg"
            end
        end
    elseif isa(msg, AdminReqMsg)
        # currently ignore the message and return a success response
        put!(rb.process.inbox, ResMsg(msg.id, STS_SUCCESS, nothing))
    elseif isa(msg, Ack2Msg)
        # remove message from already received cache
        remove_message(rb, msg)
    else
        # check for duplicates
        if isa(msg, PubSubMsg) && msg.flags == QOS2 && already_received(rb, msg)
            @info "skipping already received message $msg"
            sts = STS_SUCCESS
        else
            if isinteractive()
                sts, result = rembus_handler(rb, msg, invoke_latest)
            else
                sts, result = rembus_handler(rb, msg, invoke)
            end
        end

        if sts === STS_METHOD_EXCEPTION
            @warn "[$(msg.topic)] method error: $result"
        end
        if isa(msg, RpcReqMsg)
            response = ResMsg(msg.id, sts, result)
            @debug "response: $response"
            put!(rb.process.inbox, response)
        elseif isa(msg, PubSubMsg)
            if msg.flags > QOS0
                put!(rb.process.inbox, AckMsg(msg.id))
                if msg.flags == QOS2
                    save_message_mark(rb, msg)
                end
            end
        end
    end

    return nothing
end

function save_message_mark(rb, msg)
    DBInterface.execute(
        rb.duck.msg_stmt,
        (UInt64(msg.id >> 64), UInt64(msg.id & 0xffffffffffffffff))
    )
end

function already_received(rb, msg)
    if rb.duck === nothing
        rb.duck = rembus_db(rb.client.id)
    end

    result = DBInterface.execute(
        rb.duck.db,
        "SELECT uid FROM received WHERE ts=? AND uid=?",
        (UInt64(msg.id >> 64), UInt64(msg.id & 0xffffffffffffffff))
    )
    return !isempty(result)
end

function remove_message(rb, msg)
    if rb.duck !== nothing
        DBInterface.execute(
            rb.duck.db,
            "DELETE FROM received WHERE ts=? AND uid=?",
            (UInt64(msg.id >> 64), UInt64(msg.id & 0xffffffffffffffff))
        )
    end
end

function awaiting_ack2(rb)
    if rb.duck === nothing
        error("ack database unavailable")
    end
    DataFrame(DBInterface.execute(rb.duck.db, "SELECT * from received"))
end

#=
    parse_msg(rb, response)

Handle a received message.
=#
function parse_msg(rb, response)
    try
        msg = from_cbor(response)
        handle_input(rb, msg)
    catch e
        @error "message decoding: $e"
        @showerror e
    end

    return nothing
end

#keep_alive(rb, socket::TCPSocket) = nothing

#function keep_alive(socket::WebSockets.WebSocket)
function keep_alive(rb)
    CONFIG.ws_ping_interval == 0 && return
    while true
        sleep(CONFIG.ws_ping_interval)
        if isopen(rb.socket.io)
            if isa(rb.socket, WebSockets.WebSocket)
                ws_ping(rb)
            end
        else
            @debug "socket connection closed, keep alive done"
            break
        end
    end
end

processput!(process::NullProcess, e) = nothing

function processput!(process::Visor.Process, e)
    if getphase(process) === :up
        put!(process.inbox, e)
    end
end

function read_socket(socket, process, rb, isconnected::Condition)
    try
        rb.socket = socket
        yield()
        # signal to the initiator function _connect that the connection is up.
        notify(isconnected)

        @async keep_alive(rb)
        while isopen(socket)
            response = transport_read(socket)
            if !isempty(response)
                parse_msg(rb, response)
            else
                @debug "[$(cid(rb.client))] connection closed"
                close(socket)
            end
        end
    catch e
        @debug "[$(cid(rb.client))] connection closed: $e"
        if !isa(e, HTTP.WebSockets.WebSocketError) ||
           !isa(e.message, HTTP.WebSockets.CloseFrameBody) ||
           e.message.status != 1000
            @showerror e
            processput!(process, e)
        end
    end
end

function update_cid(rb::RBServerConnection, process, id)
    setname(process, id)
    # Search for a component with the same name
    #for (kid, cmp) in rb.router.id_twin
    filter!(rb.router.id_twin) do (kid, cmp)
        if cmp.client.id == id
            @debug "rbserver updating [$kid] -> [$id]"
            rb.client = Component(id)
            return false
        end

        return true
    end

    rb.router.id_twin[id] = rb
end

#=
Read from the socket when a component is an Acceptor.
=#
function read_socket(socket, process, rb::RBServerConnection)
    try
        while isopen(socket)
            response = transport_read(socket)
            msg = from_cbor(response)

            if isa(msg, IdentityMsg)
                ret = identity_check(rb.router, rb, msg, paging=true)
                if ret.sts === STS_SUCCESS
                    update_cid(rb, process, msg.cid)
                elseif ret.sts === STS_GENERIC_ERROR
                    @info "[$rb]: closing connection"
                    close(rb.socket)
                end
            elseif isa(msg, Attestation)
                sts = attestation(rb.router, rb, msg)
                if sts === STS_SUCCESS
                    update_cid(rb, process, msg.cid)
                end
            elseif isa(msg, Register)
                response = register(rb.router, msg)
                put!(rb.process.inbox, response)
            elseif isa(msg, Unregister)
                response = unregister(rb.router, rb, msg)
                put!(rb.process.inbox, response)
            else
                handle_input(rb, msg)
            end
        end
    catch e
        @debug "[$(cid(rb.client))] connection closed: $e"
        if isa(e, EOFError) ||
           (
            isa(e, HTTP.WebSockets.WebSocketError) &&
            isa(e.message, HTTP.WebSockets.CloseFrameBody) &&
            e.message.status == 1000
        )
        else
            @showerror e
            processput!(process, e)
        end
    finally
        shutdown(process)
    end
end

function main_task(pd, rb::RBHandle)
    try
        rb.process = pd
        setphase(pd, :wswriter)
        for msg in pd.inbox
            if isshutdown(msg)
                break
            elseif !send_message(rb, msg)
                break
            end
        end
    catch e
        @error "[$rb] write_task: $e"
    finally
        close(rb.socket)
    end
    @debug "[$(cid(rb.client))] write_task done"
end

function setup_receiver(process, socket, rb::RBConnection, isconnected)
    read_socket(socket, process, rb, isconnected)
end

brokerurl(rb::RBConnection) = brokerurl(rb.client)

function ws_connect(rb, process, isconnected::Condition)
    try
        url = brokerurl(rb)
        uri = URI(url)

        if uri.scheme == "wss"

            if !haskey(ENV, "HTTP_CA_BUNDLE")
                ENV["HTTP_CA_BUNDLE"] = rembus_ca()
            end
            @debug "cacert: $(ENV["HTTP_CA_BUNDLE"])"
            HTTP.WebSockets.open(socket -> begin
                    setup_receiver(process, socket, rb, isconnected)
                end, url)
        elseif uri.scheme == "ws"
            HTTP.WebSockets.open(socket -> begin
                    ## Sockets.nagle(socket.io.io, false)
                    ## Sockets.quickack(socket.io.io, true)
                    setup_receiver(process, socket, rb, isconnected)
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
    rb.zmqcontext = ZMQ.Context()
    rb.socket = ZMQ.Socket(rb.zmqcontext, DEALER)
    rb.socket.linger = 1
    url = brokerurl(rb)
    ZMQ.connect(rb.socket, url)
    @async zmq_receive(rb)
    return nothing
end

function tcp_connect(rb, process, isconnected::Condition)
    try
        url = brokerurl(rb)
        uri = URI(url)
        @debug "connecting to $(uri.scheme):$(uri.host):$(uri.port)"
        if uri.scheme == "tls"
            if haskey(ENV, "HTTP_CA_BUNDLE")
                cacert = ENV["HTTP_CA_BUNDLE"]
            else
                cacert = rembus_ca()
            end

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

            setup_receiver(process, ctx, rb, isconnected)
        elseif uri.scheme == "tcp"
            sock = Sockets.connect(uri.host, parse(Int, uri.port))
            setup_receiver(process, sock, rb, isconnected)
        else
            error("tcp endpoint: wrong $(uri.scheme) scheme")
        end
    catch e
        notify(isconnected, e, error=true)
    end
end

function pkfile(name)
    cfgdir = rembus_dir()
    return joinpath(cfgdir, name)
end

function resend_attestate(rb, response)
    try
        msg = attestate(rb, response)
        put!(rb.process.inbox, msg)
        if rb.client.protocol == :zmq
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> zmq_ping(rb), CONFIG.zmq_ping_interval)
        end
    catch e
        @error "resend_attestate: $e"
        @showerror e
    end

    return nothing
end

function sign(ctx::MbedTLS.PKContext, hash_alg::MbedTLS.MDKind, hash, rng)
    n = 1024 # MBEDTLS_MPI_MAX_SIZE defined in mbedtls bignum.h
    output = Vector{UInt8}(undef, n)
    len = MbedTLS.sign!(ctx, hash_alg, hash, output, rng)
    output[1:len]
end

function attestate(rb, response)
    file = pkfile(rb.client.id)
    if !isfile(file)
        error("missing/invalid $(rb.client.id) secret")
    end

    try
        ctx = MbedTLS.parse_keyfile(file)
        plain = encode([Vector{UInt8}(response.data), rb.client.id])
        hash = MbedTLS.digest(MD_SHA256, plain)
        signature = sign(ctx, MD_SHA256, hash, MersenneTwister(0))
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
    if !hasname(rb.client)
        return nothing
    end

    reason = nothing
    msg = IdentityMsg(rb.client.id)
    response = setup_request(rb, msg, request_timeout())
    if (response.status == STS_GENERIC_ERROR)
        close(rb.socket)
        throw(AlreadyConnected(rb.client.id))
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(rb, response)
        response = setup_request(rb, msg, request_timeout())
    end

    if (response.status != STS_SUCCESS)
        close(rb.socket)
        rembuserror(code=response.status, reason=reason)
    else
        if rb.client.protocol == :zmq
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> zmq_ping(rb), CONFIG.zmq_ping_interval)
        end
    end
    return nothing
end

#=
Return the CA certificate full path.

The full path is the concatenation of rembus_dir and the file present in
rembus_dir/ca
=#
function rembus_ca()
    dir = joinpath(rembus_dir(), "ca")

    if isdir(dir)
        files = readdir(dir)
        if length(files) == 1
            return joinpath(dir, files[1])
        end
    end

    throw(CABundleNotFound())
end

function connect_timeout(rb, isconnected)
    @debug "[$rb] connect timeout, socket: $(rb.socket)"
    if rb.socket === nothing
        notify(isconnected, ErrorException("connection failed"), error=true)
    end
end

protocol(rb::RBConnection) = rb.client.protocol

function _connect(rb, prc)
    proto = protocol(rb)
    if proto === :ws || proto === :wss
        isconnected = Condition()
        t = Timer((tim) -> connect_timeout(rb, isconnected), connect_request_timeout())
        @async ws_connect(rb, prc, isconnected)
        wait(isconnected)
        close(t)
    elseif proto === :tcp || proto === :tls
        isconnected = Condition()
        @async tcp_connect(rb, prc, isconnected)
        wait(isconnected)
    elseif proto === :zmq
        zmq_connect(rb)
    else
        throw(ErrorException(
            "wrong protocol $proto: must be tcp|tls|zmq|ws|wss"
        ))
    end

    @async write_task(rb)
    return rb
end

function _connect(rb)
    prc = process(cid(rb.client), main_task, args=(rb,))
    supervise([prc], wait=false)
    yield()
    return _connect(rb, prc)
end

function ws_ping(rb)
    try
        put!(rb.process.inbox, WsPing())
    catch e
        @info "socket ping: $e"
    end

    return nothing
end

function rembus_write(rb::RBHandle, msg)
    @debug ">> [$(cid(rb.client))] $(isdefined(msg, :id) ? msg.id : "") -> $msg"
    return transport_send(Val(rb.type), rb, msg)
end

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

isconnected(p::Visor.Process) = isconnected(p.args[1])

function connect(rb::RBConnection)
    if !isconnected(rb)
        if rb.client.protocol !== :zmq && CONFIG.connection_mode === authenticated
            if !hasname(rb.client)
                close(rb)
                error("anonymous components not allowed")
            end
            task = @async connection_inquiry(rb)
            _connect(rb)
            wait(task)
        else
            _connect(rb)
            authenticate(rb)
        end
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
    rb = RBConnection(url)
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
    callbacks(rb, rb.receiver, rb.subinfo)
    return rb
end

function bind(process::Visor.Supervised, rb::RBServerConnection)
    #@async write_task(rb)
    server = rb.router
    callbacks(rb, server.topic_function, server.subinfo)
    return rb
end

#=
Notify all subscribed and exposed method to the remote node.
This happens just after a connection establishement.
=#
function callbacks(rb::RBHandle, fnmap, submap)
    for (name, fn) in fnmap
        if haskey(submap, name)
            subscribe_server(rb, name, from=submap[name])
        else
            expose_server(rb, name)
        end
    end

    if rb.reactive
        reactive_server(rb)
    end
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

function connect(urls::Vector, policy=:policy_default)
    pool = RBPool(policy, [RBConnection(url) for url in urls])
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

function Base.close(rb::RBHandle)
    if rb.socket !== nothing
        # TODO: check if race conditions may arise
        #put!(rb.process.inbox, CloseConnection())

        if isa(rb.socket, ZMQ.Socket)
            transport_send(Val(rb.type), rb, Close())
            close(rb.socket)
            close(rb.zmqcontext)
        else
            close(rb.socket)
        end
        rb.socket = nothing
    end
    return nothing
end

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
    reactive(
        rb::RBHandle;
        from::Union{Real,Period,Dates.CompoundPeriod}=Day(1),
        exceptionerror=true
    )

Start the delivery of published messages for which there was declared
an interest with [`subscribe`](@ref).
"""
function reactive(
    rb::RBHandle;
    from::Union{Real,Period,Dates.CompoundPeriod}=Day(1),
    timeout=request_timeout(),
    exceptionerror=true
)
    response = rpcreq(
        rb,
        AdminReqMsg(
            BROKER_CONFIG,
            Dict(
                COMMAND => REACTIVE_CMD,
                STATUS => true,
                MSG_FROM => to_microseconds(from))
        ),
        exceptionerror=exceptionerror,
        timeout=timeout,
        broadcast=true
    )
    rb.reactive = true

    return response
end

#=
The from keyword of the subscribe methods may assume the values:
  * Now() subscribes for messages received from now on;
  * LastReceived() subscribes for all messages received in the past where node was offline;
=#
Now() = 0.0
LastReceived() = Inf

function to_microseconds(msg_from::Union{Real,Period,Dates.CompoundPeriod})
    if isa(msg_from, Real)
        return msg_from
    elseif isa(msg_from, Period)
        return Microsecond(msg_from).value
    elseif isa(msg_from, Dates.CompoundPeriod)
        return sum(Microsecond.(msg_from.periods)).value
    end
end

"""
    subscribe(rb::RBHandle, fn::Function; from=Now(), exceptionerror=true)
    subscribe(
        rb::RBHandle, topic::AbstractString, fn::Function; from=Now(),
        exceptionerror=true
    )

Declare interest for messages published on `topic` logical channel.

The function `fn` is called when a message is received on `topic` and
[`reactive`](@ref) put the `rb` component in reactive mode.

If the `topic` argument is omitted the function name must be equal to the topic name.

If `from` is `LastReceived()` then `rb` component will receive messages published when it was
offline.
"""
function subscribe(
    rb::RBConnection, topic::AbstractString, fn::Function;
    from::Union{Real,Period,Dates.CompoundPeriod}=Now(), exceptionerror=true
)
    add_receiver(rb, topic, fn)
    rb.subinfo[topic] = to_microseconds(from)
    return rpcreq(rb,
        AdminReqMsg(
            topic,
            Dict(COMMAND => SUBSCRIBE_CMD, MSG_FROM => rb.subinfo[topic])
        ),
        exceptionerror=exceptionerror,
        broadcast=true
    )
end

#=
Subscribe the topic to the remote node, tipically a broker.

TODO improve docs and make tests

The argument fn is not needed because a server component
doesn't require

=#
function subscribe(
    rb::RBServerConnection, topic::AbstractString, fn::Function;
    from::Union{Real,Period,Dates.CompoundPeriod}=Now(), exceptionerror=true
)
    return rpcreq(rb,
        AdminReqMsg(
            topic,
            Dict(COMMAND => SUBSCRIBE_CMD, MSG_FROM => to_microseconds(from))
        ),
        exceptionerror=exceptionerror
    )
end

function subscribe(
    rb::RBHandle, fn::Function; from=Now(), exceptionerror=true
)
    return subscribe(
        rb, string(fn), fn; from=from, exceptionerror=exceptionerror
    )
end

"""
    component(url)

Connect rembus component defined by `url`.

The connection is supervised and network faults starts connection retries attempts
until successful outcome.
"""
function component(url=getcomponent())
    rb = Rembus.RBConnection(url)
    return component(rb)
end

function component(rb::RBConnection)
    p = process(cid(rb), Rembus.client_task,
        args=(rb,), debounce_time=2, restart=:transient)

    supervise(
        p, intensity=3, wait=false
    )
    yield()
    return p
end

function component(rb::RBPool)
    p = process(cid(rb), Rembus.pool_task,
        args=(rb,), debounce_time=2, restart=:transient)

    supervise(
        p, intensity=3, wait=false
    )
    yield()
    return p
end

"""
    component(urls::Vector)

Connect component to remotes defined be `urls` array.

The connection pool is supervised.
"""
function component(urls::Vector, policy=:policy_default)
    pool = RBPool(policy, [RBConnection(url) for url in urls])
    return component(pool)
end

terminate(proc::Visor.Process) = shutdown(proc)

function expose(proc::Visor.Process, fn::Function)
    return call(proc, Rembus.AddImpl(fn), timeout=call_timeout())
end

function expose(proc::Visor.Process, topic::AbstractString, fn::Function)
    return call(proc, Rembus.AddImpl(topic, fn), timeout=call_timeout())
end

function unexpose(proc::Visor.Process, fn)
    return call(proc, Rembus.RemoveImpl(fn), timeout=call_timeout())
end

function subscribe(proc::Visor.Process, fn::Function; from=Now())
    return call(proc, Rembus.AddInterest(fn, from), timeout=call_timeout())
end

function unsubscribe(proc::Visor.Process, fn)
    return call(proc, Rembus.RemoveInterest(fn), timeout=call_timeout())
end

function subscribe(
    proc::Visor.Process, topic::AbstractString, fn::Function; from=Now()
)
    return call(proc, Rembus.AddInterest(topic, fn, from), timeout=call_timeout())
end

function reactive(proc::Visor.Process, from=LastReceived())
    return call(proc, Reactive(true, from), timeout=call_timeout())
end

function unreactive(proc::Visor.Process)
    return call(proc, Reactive(false, NaN), timeout=call_timeout())
end

function shared(proc::Visor.Process, ctx)
    return call(proc, SetHolder(ctx), timeout=call_timeout())
end

function publish(proc::Visor.Process, topic::AbstractString, data=[]; qos=QOS0)
    cast(proc, CastCall(topic, data, qos))
end

function rpc(proc::Visor.Process, topic::AbstractString, data=[])
    return call(proc, RpcReqMsg(topic, data), timeout=call_timeout())
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
        exceptionerror=exceptionerror,
        broadcast=true
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
        exceptionerror=exceptionerror,
        broadcast=true
    )
end

function expose(rb::RBHandle, fn::Function; exceptionerror=true)
    return expose(rb, string(fn), fn; exceptionerror=exceptionerror)
end

function expose_server(rb::RBHandle, topic::AbstractString)
    return to_socket(rb, AdminReqMsg(topic, Dict(COMMAND => EXPOSE_CMD)))
end

function subscribe_server(
    rb::RBServerConnection, topic::AbstractString;
    from::Union{Real,Period,Dates.CompoundPeriod}=Now()
)
    rb.router.subinfo[topic] = to_microseconds(from)
    return to_socket(
        rb,
        AdminReqMsg(
            topic, Dict(COMMAND => SUBSCRIBE_CMD, MSG_FROM => rb.router.subinfo[topic])
        )
    )
end

function subscribe_server(
    rb::RBConnection, topic::AbstractString;
    from::Union{Real,Period,Dates.CompoundPeriod}=Now()
)
    rb.subinfo[topic] = to_microseconds(from)
    return to_socket(
        rb,
        AdminReqMsg(
            topic, Dict(COMMAND => SUBSCRIBE_CMD, MSG_FROM => rb.subinfo[topic])
        )
    )
end

function reactive_server(
    rb::RBHandle;
    from::Union{Real,Period,Dates.CompoundPeriod}=Day(1)
)
    to_socket(
        rb,
        AdminReqMsg(
            BROKER_CONFIG,
            Dict(
                COMMAND => REACTIVE_CMD,
                STATUS => true,
                MSG_FROM => to_microseconds(from)
            )
        )
    )
    rb.reactive = true
    return nothing
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
        exceptionerror=exceptionerror,
        broadcast=true
    )
end

function unexpose(rb::RBHandle, fn::Function; exceptionerror=true)
    return unexpose(rb, string(fn), exceptionerror=exceptionerror)
end

"""
    private_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)

Set the `topic` to private.

The component must have the admin role for changing the privateness level.
"""
function private_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => PRIVATE_TOPIC_CMD)),
        exceptionerror=exceptionerror
    )
end

"""
    public_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)

Set the `topic` to public.

The component must have the admin role for changing the privateness level.
"""
function public_topic(rb::RBHandle, topic::AbstractString; exceptionerror=true)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => PUBLIC_TOPIC_CMD)),
        exceptionerror=exceptionerror
    )
end

"""
    function authorize(
        rb::RBHandle, client::AbstractString, topic::AbstractString;
        exceptionerror=true
    )

Authorize the `client` component to use the private `topic`.

The component must have the admin role for granting topic accessibility.
"""
function authorize(
    rb::RBHandle, client::AbstractString, topic::AbstractString;
    exceptionerror=true
)
    return rpcreq(rb,
        AdminReqMsg(topic, Dict(COMMAND => AUTHORIZE_CMD, CID => client)),
        exceptionerror=exceptionerror
    )
end

"""
    function unauthorize(
        rb::RBHandle, client::AbstractString, topic::AbstractString;
        exceptionerror=true
    )

Revoke authorization to the `client` component for use of the private `topic`.

The component must have the admin role for revoking topic accessibility.
"""
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
#     zmq_ping(rb::RBHandle)
#
# Send a ping message to check if the broker is online.
#
# Required by ZeroMQ socket.
# """
function zmq_ping(rb::RBConnection)
    try
        if rb.client.protocol == :zmq
            if isconnected(rb)
                rpcreq(rb, PingMsg(rb.client.id))
            end
            CONFIG.zmq_ping_interval > 0 && Timer(tmr -> zmq_ping(rb), CONFIG.zmq_ping_interval)
        end
    catch e
        @debug "[$(cid(rb.client))]: pong not received"
        @showerror e
    end

    return nothing
end

"""
    publish(rb::RBHandle, topic::AbstractString, data=[]; qos=QOS0)

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
function publish(rb::RBConnection, topic::AbstractString, data=[]; qos=QOS0)
    publish(rb, PubSubMsg(topic, data, qos))
    return nothing
end

function publish(rb::RBConnection, msg::PubSubMsg)
    put!(rb.process.inbox, msg)
    return nothing
end

function publish(rb::RBPool, topic::AbstractString, data=[]; qos=QOS0)
    msg = PubSubMsg(topic, data, qos)
    conn = pick_connections(rb, msg)
    for c in conn
        publish(c, topic, data, qos=qos)
    end

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

function rpc_future(rb::RBHandle, topic::AbstractString, data=[];
    exceptionerror=true, timeout=request_timeout())
    rpcreq(
        rb,
        RpcReqMsg(topic, data),
        exceptionerror=exceptionerror,
        timeout=timeout,
        wait=false
    )
end

function fetch_response(f::Distributed.Future)
    response = fetch(f)
    if response.status == STS_SUCCESS
        return response.data
    else
        throw(RembusError(code=response.status, reason=response.data))
    end
end

function direct(
    rb::RBHandle, target::AbstractString, topic::AbstractString, data=nothing;
    exceptionerror=true
)
    return rpcreq(rb, RpcReqMsg(topic, data, target), exceptionerror=exceptionerror)
end

function response_timeout(rb, condition::Distributed.Future, msg::RembusMsg)
    if hasproperty(msg, :topic)
        descr = "[$(msg.topic)]: request timeout"
    else
        descr = "[$msg]: request timeout"
    end

    put!(condition, RembusTimeout(descr))
    delete!(rb.out, msg.id)

    return nothing
end

#=
Send a message when the rembus task is initializing and it si not yet
ready to pull messages from inbox.
=#
function setup_request(rb::RBHandle, msg::RembusMsg, timeout)
    ph = phase(rb)
    if ph === :wswriter
        resp_cond = send_request(rb, msg)
    else
        resp_cond = to_socket(rb, msg)
        yield()
    end

    t = Timer((tim) -> response_timeout(rb, resp_cond, msg), timeout)
    response = fetch(resp_cond)
    close(t)

    if isa(response, RembusTimeout)
        rb.socket !== nothing && close(rb.socket)
        throw(response)
    end

    return response
end

function inquiry_timeout(rb, condition::Distributed.Future)
    put!(condition, RembusTimeout("inquiry timeout"))
    delete!(rb.out, CONNECTION_ID)
    return nothing
end

function send_request(rb::RBHandle, msg::RembusMsg)
    mid::UInt128 = msg.id
    resp_cond = Distributed.Future()
    rb.out[mid] = resp_cond
    put!(rb.process.inbox, msg)
    return resp_cond
end

#=
Write the message directly to the socket, bypassing the process dedicated
to send the messages.

To be used only for the initial setup phase.
=#
function to_socket(rb::RBHandle, msg::RembusMsg)
    mid::UInt128 = msg.id
    resp_cond = Distributed.Future()
    rb.out[mid] = resp_cond
    send_message(rb, msg)
    return resp_cond
end

function send_message(rb::RBHandle, msg)
    #    if isa(msg, CloseConnection)
    #        if rb.socket !== nothing
    #            if isa(rb.socket, ZMQ.Socket)
    #                transport_send(Val(rb.type), rb, Close())
    #                close(rb.zmqcontext)
    #            else
    #                close(rb.socket)
    #            end
    #        end
    #
    #        return false
    #    elseif isa(msg, WsPing)
    if isa(msg, WsPing)
        WebSockets.ping(rb.socket)
    else
        if rb.egress !== nothing
            msg = rb.egress(rb, msg)
        end
        if msg !== nothing
            outcome = false
            while !outcome
                # in the case of pubsub message
                # retry until the ack message is received
                outcome = rembus_write(rb, msg)
            end
        end
    end

    return true
end

#=
A broker with ConnectionMode equal to authenticated send immediately
a challenge.
=#
function connection_inquiry(rb::RBHandle)
    resp_cond = Distributed.Future()
    rb.out[CONNECTION_ID] = resp_cond
    t = Timer((tim) -> inquiry_timeout(rb, resp_cond), request_timeout())
    response = fetch(resp_cond)
    close(t)

    if isa(response, RembusTimeout)
        @info "[$rb] no inquiry from acceptor"
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(rb, response)
        response = setup_request(rb, msg, request_timeout())
    end

    return nothing
end

function phase(rb::RBHandle)
    if isdefined(rb, :process)
        return getphase(rb.process)
    end
    return :no_process
end

# https://github.com/JuliaLang/julia/issues/36217
function wait_response(rb::RBHandle, msg::RembusMsg, timeout)
    resp_cond = send_request(rb, msg)
    yield()
    t = Timer((tim) -> response_timeout(rb, resp_cond, msg), timeout)
    res = fetch(resp_cond)
    close(t)
    return res
end

function pick_connections(handle::RBPool, msg)
    if handle.policy === :policy_first_up
        return first_up(handle, msg.topic, handle.connections)
    elseif handle.policy === :policy_round_robin
        return round_robin(handle, msg.topic, handle.connections)
    elseif handle.policy === :policy_less_busy
        return less_busy(handle, msg.topic, handle.connections)
    elseif handle.policy === :policy_all
        return handle.connections
    elseif isa(msg, PubSubMsg)
        # pick all if it is a publish message
        return handle.connections
    else
        return first_up(handle, msg.topic, handle.connections)
    end
end

function do_request(
    rb::RBHandle, msg::RembusMsg, wait::Bool, timeout, exceptionerror
)
    if wait
        response = wait_response(rb, msg, timeout)
        return get_response(rb, msg, response, exceptionerror=exceptionerror)
    else
        return send_request(rb, msg)
    end
end

function do_request(
    rbs::Vector{RBConnection}, msg::RembusMsg, wait::Bool, timeout, exceptionerror
)
    responses = []
    for rb in rbs
        if !isconnected(rb)
            push!(responses, missing)
        elseif wait
            response = wait_response(rb, msg, timeout)
            result = get_response(rb, msg, response, exceptionerror=exceptionerror)
            value = isa(result, RembusTimeout) ? missing : result
            push!(
                responses,
                value
            )
        else
            push!(responses, send_request(rb, msg))
        end
    end
    return responses
end

# Send a RpcReqMsg message to rembus and return the response.
# If broadcast is true send the request to all nodes of the pool.
function rpcreq(
    handle::RBHandle, msg;
    exceptionerror=true,
    timeout=request_timeout(),
    wait=true,
    broadcast=false
)
    !isconnected(handle) && error("connection is down")
    if isa(handle, RBPool)
        if broadcast
            conn = handle.connections
        else
            conn = pick_connections(handle::RBPool, msg)
        end
    else
        conn = handle
    end

    return do_request(conn, msg, wait, timeout, exceptionerror)
end

function get_response(rb, msg, response; exceptionerror=true)
    outcome = nothing
    if isa(response, RembusTimeout)
        outcome = response
        if exceptionerror
            throw(outcome)
        end
    elseif response.status == STS_SUCCESS
        outcome = response.data
    elseif response.status == STS_CHALLENGE
        @async resend_attestate(rb, response)
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
    forever(rb::Visor.Process)

    Start the event loop awaiting to execute exposed and subscribed methods.
"""
function forever(rb::Visor.Process)
    reactive(rb)
    if !isinteractive()
        wait(Visor.root_supervisor(rb))
    end
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
    rembus_dir!("/tmp")
    ENV["REMBUS_ZMQ_PING_INTERVAL"] = "0"
    ENV["REMBUS_WS_PING_INTERVAL"] = "0"
    ENV["REMBUS_TIMEOUT"] = "20"
    ENV["REMBUS_CONNECT_TIMEOUT"] = "20"
    @compile_workload begin
        sv = Rembus.broker(
            wait=false,
            mode="anonymous",
            log="error",
            ws=8000,
            tcp=8001,
            zmq=8002,
            http=9000,
            reset=true
        )
        yield()
        Rembus.islistening(wait=20)
        include("precompile.jl")
        shutdown()
    end
    rembus_dir!(default_rembus_dir())
end

end # module
