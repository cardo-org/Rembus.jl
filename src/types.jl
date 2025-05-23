abstract type RembusException <: Exception end

"""
$(TYPEDEF)

Exception thrown when a response is not received before the request timeout expires.

## Fields
$(TYPEDFIELDS)
"""
struct RembusTimeout{T} <: RembusException
    "request message"
    msg::T
    RembusTimeout{T}(msg) where {T} = new{T}(msg)
end

RembusTimeout(msg) = RembusTimeout{typeof(msg)}(msg)

"""
$(TYPEDEF)

Generic Rembus error.

## Fields
$(TYPEDFIELDS)
"""
Base.@kwdef struct RembusError <: RembusException
    "error code"
    code::UInt8
    "topic name if available"
    topic::Union{String,Nothing} = nothing
    "detailed error message"
    reason::Union{String,Nothing} = nothing
end

"""
$(TYPEDEF)

Exception thrown from a rpc request when the called method is unknown.

## Fields
$(TYPEDFIELDS)

## Example
An RPC Client request a method that does not exist.

```julia
@rpc coolservice()
```

The result is an exception:

```
ERROR: Rembus.RpcMethodNotFound("rembus", "coolservice")
Stacktrace:
...
```
"""
struct RpcMethodNotFound <: RembusException
    "service name"
    topic::String
end

"""
$(TYPEDEF)

Thrown when a RPC method is unavailable.

A method is considered unavailable when some component that exposed the method is
currently disconnected from the broker.

## Fields
$(TYPEDFIELDS)
"""
struct RpcMethodUnavailable <: RembusException
    "service name"
    topic::String
end

"""
$(TYPEDEF)

Thrown when a RPC request would invoke a locally exposed method.

## Fields
$(TYPEDFIELDS)
"""
struct RpcMethodLoopback <: RembusException
    "service name"
    topic::String
end

"""
$(TYPEDEF)

Thrown when a RPC method throws an exception.

## Fields
$(TYPEDFIELDS)

## Example
A component exposes a method that expect a string argument.

```julia
@expose foo(name::AbstractString) = "hello " * name
```

A RPC client invoke the method with an integer argument.

```julia
try
    @rpc foo(1)
catch e
    @error e.reason
end
```

The result is an exception:

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
    "service name"
    topic::String
    "remote exception description"
    reason::String
end

@enum NodeStatus up down unknown

# Available modes of connection.
# if mode is authenticated then anonymous modes are not permitted.
# if mode is anonymous all connection modes are available.
@enum ConnectionMode anonymous authenticated

mutable struct RbURL
    id::String
    tenant::String
    hasname::Bool
    protocol::Symbol
    host::String
    port::UInt16
    props::Dict{String,String}
    function RbURL(;
        name="",
        protocol=:ws,
        host="127.0.0.1",
        port=8000,
        hasname=true,
        props=Dict()
    )
        if protocol === :repl
            return new("__repl__", ".", false, :repl, "", 0, props)
        else
            if isempty(name)
                name = string(uuid4())
                hasname = false
            end
            return new(name, domain(name), hasname, protocol, host, port, props)
        end
    end
    function RbURL(url::String)
        (cid, tenant, hasname, protocol, host, port, props) = spliturl(url)
        new(cid, tenant, hasname, protocol, host, port, props)
    end
end

nodeurl(c::RbURL) = "$(c.protocol == :zmq ? :tcp : c.protocol)://$(c.host):$(c.port)"

rid(c::RbURL) = c.id

function cid(c::RbURL)
    c.protocol === :repl ? "__repl__" : "$(c.protocol)://$(c.host):$(c.port)/$(c.id)"
end

hasname(c::RbURL) = c.hasname

isrepl(c::RbURL) = c.protocol === :repl

struct AckState
    ack2::Bool
    timer::Timer
end

ack_dataframe() = DataFrame(ts=UInt64[], id=UInt128[])

# Wrong tcp packet received.
struct WrongTcpPacket <: Exception
end

struct CABundleNotFound <: Exception
end

#=
A component of the Rembus network.

If port is equal to zero the node is not eligible to become a broker.
=#
struct Node
    cid::String # component id
    tenant::String
    protocol::Symbol # :ws, :wss, :tcp, :tls, :zmq
    host::String  # hostname or ip address
    port::UInt16  # listening port
    status::NodeStatus
    Node(cid, proto, host, port, sts) = begin
        tenant = domain(cid)
        new(cid, tenant, proto, host, port, sts)
    end
    function Node(url)
        (cid, tenant, _, protocol, host, port, _) = spliturl(url)
        new(cid, tenant, protocol, host, port, unknown)
    end
end

function nodes(cid::String, source_address::String, portmap::Dict)
    res = []
    for (proto, port) in portmap
        push!(res, Node(cid, Symbol(proto), source_address, port, up))
    end

    return res
end

function Base.show(io::IO, n::Node)
    print(io, "$(n.cid) -> $(n.protocol)://$(n.host):$(n.port) [$(n.status)]")
end

struct FutureResponse{T}
    future::Channel
    sending_ts::Float64
    request::T
    timer::Timer
    FutureResponse(request, timer) = new{typeof(request)}(
        Channel(1),
        time(),
        request,
        timer
    )
end

abstract type AbstractSocket end

abstract type AbstractPlainSocket <: AbstractSocket end

struct Float <: AbstractSocket
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    Float(out=Dict(), direct=Dict()) = new(out, direct)
end

Base.show(io::IO, s::Float) = print(io, "FLOAT")

struct WS <: AbstractPlainSocket
    sock::HTTP.WebSockets.WebSocket
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    WS(sock) = new(
        sock,
        Dict(), # out
        Dict(), # direct
    )
end

Base.show(io::IO, s::WS) = print(io, "WS:$(isopen(s))")

struct TCP <: AbstractPlainSocket
    sock::Sockets.TCPSocket
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    TCP(sock) = new(
        sock,
        Dict(), # out
        Dict(), # direct
    )
end

Base.show(io::IO, s::TCP) = print(io, "TCP:$(isopen(s))")

struct TLS <: AbstractPlainSocket
    sock::MbedTLS.SSLContext
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    TLS(sock) = new(
        sock,
        Dict(), # out
        Dict(), # direct
    )
end

Base.show(io::IO, s::TLS) = print(io, "TLS:$(isopen(s))")

struct ZDealer <: AbstractSocket
    sock::ZMQ.Socket
    context::ZMQ.Context
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    function ZDealer()
        context = ZMQ.Context()
        sock = ZMQ.Socket(context, DEALER)
        sock.linger = 1
        return new(
            sock,
            context,
            Dict(), # out
            Dict(), # direct
        )
    end
end

struct ZRouter <: AbstractSocket
    sock::ZMQ.Socket
    zaddress::Vector{UInt8}
    out::Dict{UInt128,FutureResponse}
    direct::Dict{UInt128,FutureResponse}
    ZRouter(sock, address) = new(
        sock,
        address,
        Dict(), # out
        Dict(), # direct
    )
end

Base.isopen(ws::WebSockets.WebSocket) = isopen(ws.io)

Base.isopen(endpoint::AbstractSocket) = isopen(endpoint.sock)

Base.isopen(endpoint::Float) = false # COV_EXCL_LINE

Base.close(::AbstractSocket) = nothing
Base.close(endpoint::AbstractPlainSocket) = close(endpoint.sock)

function Base.close(endpoint::ZDealer)
    transport_send(endpoint, Close())
    close(endpoint.sock)
    close(endpoint.context)
end

const FLOAT = Float()

@enum ListenerStatus on off

struct WsPing end

mutable struct Listener
    status::ListenerStatus
    port::UInt
    Listener(port) = new(off, port)
end

struct RembusMetrics
    rpc::Prometheus.Family{Prometheus.Histogram}
    pub::Prometheus.Family{Prometheus.Counter}
    RembusMetrics(reg) = new(
        Prometheus.Family{Prometheus.Histogram}(
            "broker_rpc",
            "Round trip time for rpc requests",
            ("topic",),
            registry=reg
        ),
        Prometheus.Family{Prometheus.Counter}(
            "broker_pub",
            "Count of published messages",
            ("topic",),
            registry=reg
        )
    )
end

mutable struct Settings
    zmq_ping_interval::Float32
    ws_ping_interval::Float32
    rembus_dir::String
    overwrite_connection::Bool
    stacktrace::Bool  # log stacktrace on error
    connection_retry_period::Float32 # seconds between reconnection attempts
    broker_plugin::Union{Nothing,Module}
    save_messages::Bool
    cache_size::UInt
    connection_mode::ConnectionMode
    request_timeout::Float64
    challenge_timeout::Float64
    ack_timeout::Float64
    reconnect_period::Float64
    Settings(name::AbstractString) = begin
        cfg = getcfg(name)

        zmq_ping_interval = get(cfg, "zmq_ping_interval",
            parse(Float32, get(ENV, "REMBUS_ZMQ_PING_INTERVAL", "30")))

        ws_ping_interval = get(cfg, "ws_ping_interval",
            parse(Float32, get(ENV, "REMBUS_WS_PING_INTERVAL", "30")))
        rdir = get(cfg, "rembus_dir", get(ENV, "REMBUS_DIR", default_rembus_dir()))

        overwrite_connection = get(cfg, "overwrite_connection", false)
        stacktrace = get(cfg, "stacktrace", false)

        connection_mode = string_to_enum(get(cfg, "connection_mode", "anonymous"))
        connection_retry_period = get(cfg, "connection_retry_period", 2.0)

        cache_size = get(
            cfg,
            "cache_size",
            parse(UInt, get(ENV, "REMBUS_CACHE_SIZE", REMBUS_CACHE_SIZE))
        )
        request_timeout = get(
            cfg,
            "request_timeout",
            parse(Float64, get(ENV, "REMBUS_TIMEOUT", "5"))
        )
        challenge_timeout = get(
            cfg,
            "challenge_timeout",
            parse(Float64, get(ENV, "REMBUS_CHALLENGE_TIMEOUT", "3"))
        )
        ack_timeout = get(
            cfg,
            "ack_timeout",
            parse(Float64, get(ENV, "REMBUS_ACK_TIMEOUT", "2"))
        )
        reconnect_period = get(
            cfg,
            "reconnect_period",
            parse(Float64, get(ENV, "REMBUS_RECONNECT_PERIOD", "1"))
        )
        new(
            zmq_ping_interval,
            ws_ping_interval,
            rdir,
            overwrite_connection,
            stacktrace,
            connection_retry_period,
            nothing,
            true,
            cache_size,
            connection_mode,
            request_timeout,
            challenge_timeout,
            ack_timeout,
            reconnect_period
        )
    end
end

abstract type AbstractRouter end

abstract type AbstractTwin end

mutable struct Router{T<:AbstractTwin} <: AbstractRouter
    upstream::Union{Nothing,AbstractRouter}
    downstream::Union{Nothing,Rembus.AbstractRouter}
    id::String
    eid::UInt64 # ephemeral unique id
    settings::Settings
    mode::ConnectionMode
    lock::ReentrantLock
    policy::Symbol
    metrics::Union{Nothing,RembusMetrics}
    msg_df::DataFrame
    mcounter::UInt64
    network::Vector{Node}
    start_ts::Float64
    servers::Set{String}
    listeners::Dict{Symbol,Listener} # protocol => listener status
    address2twin::Dict{Vector{UInt8},T} # zeromq address => twin
    plugin::Union{Nothing,Module}
    shared::Any
    topic_impls::Dict{String,OrderedSet{T}} # topic => twins implementor
    last_invoked::Dict{String,Int} # topic => twin index last called
    topic_interests::Dict{String,Set{T}} # topic => twins subscribed to topic
    id_twin::Dict{String,T} # id => twin
    topic_function::Dict{String,Function}
    subinfo::Dict{String,Float64}
    topic_auth::Dict{String,Dict{String,Bool}} # topic => {rid(twin) => true}
    admins::Set{String}
    tcp_server::Sockets.TCPServer
    http_server::HTTP.Server
    ws_server::Sockets.TCPServer
    zmqsocket::ZMQ.Socket
    zmqcontext::ZMQ.Context
    process::Visor.Process
    owners::Dict{String,String}
    Router{T}(name, plugin=nothing, context=missing) where {T<:AbstractTwin} = new{T}(
        nothing,
        nothing,
        name,
        rand(Xoshiro(time_ns()), UInt64),
        Settings(name),
        anonymous,
        ReentrantLock(),
        :first_up,
        nothing,
        msg_dataframe(),
        0,
        [],
        time(), # start_ts
        Set(),
        Dict(),
        Dict(),
        plugin,
        context, # shared
        Dict(), # topic_impls
        Dict(), # last_invoked
        Dict(), # topic_interests
        Dict(), # id_twin
        Dict(), # topic_function
        Dict(), # subinfo
        Dict(), # topic_auth
        Set(), # admins
    )
end

function upstream!(router, upstream_router)
    router.upstream = upstream_router
    upstream_router.downstream = router
end

function last_downstream(router)
    while !isnothing(router.downstream)
        router = router.downstream
    end

    return router
end

function first_upstream(router)
    while !isnothing(router.upstream)
        router = router.upstream
    end

    return router
end

mutable struct Twin <: AbstractTwin
    uid::RbURL
    connected::Union{Nothing,Channel{Bool}}
    shared::Any
    handler::Dict{String,Function}
    isauth::Bool
    reactive::Bool
    egress::Union{Nothing,Function}
    ingress::Union{Nothing,Function}
    router::AbstractRouter
    socket::AbstractSocket
    mark::UInt64
    msg_from::Dict{String,Float64} # subtract from now and consider minimum ts of unsent msg
    probe::Bool
    failovers::Vector{RbURL}
    failover_from::Float64
    ackdf::DataFrame
    process::Visor.Process
    Twin(uid::RbURL, r::AbstractRouter, s=FLOAT) = new(
        uid,
        nothing,
        missing,
        Dict(), # handler
        false,
        false,
        nothing,
        nothing,
        r,
        s,
        0,
        Dict(), # msg_from
        false,
        [],
        0.0
    )
end

Base.:(==)(a::Twin, b::Twin) = rid(a) === rid(b)

Base.hash(t::Twin) = hash(rid(t))

"""
$(TYPEDSIGNATURES)
Return the identifier of the component (`R`embus `ID`entifier).

```julia
rb = component("ws://myhost.org:8000/myname")
rid(rb) === "myname"
```
"""
rid(rb::Twin)::String = rb.uid.id

cid(twin::Twin) = cid(twin.uid)

nodeurl(rb::Twin) = nodeurl(rb.uid)

path(t::Twin) = "$(isdefined(t, :process) ? t.process.supervisor.supervisor : ":"):$(rid(t))"

hasname(twin::Twin) = hasname(twin.uid)

# TODO: to be called iszmqdealer
iszmq(twin::Twin) = isa(twin.socket, ZDealer)

failover_queue(twin::Twin) = twin.failover_from > 0.0

domain(twin::Twin) = twin.uid.tenant

"""
$(TYPEDSIGNATURES)
Block [`rpc`](@ref) and [`publish`](@ref) calls until the twin is connected to the broker.
"""
function ifdown_block(rb::Twin)
    rb.connected = Channel{Bool}(1)
    return nothing
end

function failover_queue!(twin::Twin, topic::AbstractString; msg_from=Inf)
    twin.failover_from = msg_from
    twin.msg_from[topic] = msg_from
    send_queue(twin, twin.failover_from)
    return nothing
end

#=
    offline!(router, twin)

Unbind the ZMQ socket from the twin.
=#
function offline!(router::Router, twin::Twin)
    @debug "[$twin] closing: going offline"
    twin.socket = Float()
    # Remove from address2twin

    filter!(((k, v),) -> twin != v, router.address2twin)
    return nothing
end

function wait_open(twin::Twin)
    if !isnothing(twin.connected)
        return fetch(twin.connected)
    else
        return isopen(twin)
    end
end

"""
$(TYPEDSIGNATURES)
Check if the component is connected to the broker.
"""
function Base.isopen(rb::Twin)::Bool
    if isrepl(rb.uid)
        return isdefined(rb, :process) && !istaskdone(rb.process.task)
    else
        return isopen(rb.socket)
    end
end

"""
$(TYPEDSIGNATURES)
Wait for RPC requests and Pub/Sub messages.
"""
Base.wait(rb::Twin) = isdefined(rb, :process) ? wait(rb.process.task) : nothing

isconnected(twin::Twin) = isopen(twin.socket)

function isconnected(router::Router, twin_id)
    return haskey(router.id_twin, twin_id) && isconnected(router.id_twin[twin_id])
end

protocol(twin::Twin) = twin.uid.protocol

Base.show(io::IO, r::Router) = print(io, "$(r.id)")
Base.show(io::IO, t::Twin) = print(io, "$(path(t))")

msg_dataframe() = DataFrame(
    ptr=UInt[], ts=UInt[], uid=UInt128[], topic=String[], pkt=Vector{UInt8}[]
)
mutable struct RouterCollector <: Prometheus.Collector
    router::Router
    function RouterCollector(
        router::Router;
        registry::Union{Prometheus.CollectorRegistry,Nothing}=Prometheus.DEFAULT_REGISTRY
    )
        coll = new(router)
        if registry !== nothing
            # ignore already registered error
            try
                Prometheus.register(registry, coll)
            catch
            end
        end
        return coll
    end
end

function Prometheus.metric_names(::RouterCollector)
    return (
        "broker_websocket_connections",
    )
end

function Prometheus.collect!(metrics::Vector, rc::RouterCollector)
    connected_clients = 0
    for tw in values(rc.router.id_twin)
        if isopen(tw)
            connected_clients += 1
        end
    end

    push!(
        metrics,
        Prometheus.Metric(
            "gauge",
            "broker_websocket_connections",
            "Total number of WebSocket connections",
            Prometheus.Sample(nothing, nothing, nothing, connected_clients)))

    return metrics
end
