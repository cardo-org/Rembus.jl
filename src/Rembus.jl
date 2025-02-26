module Rembus

using Arrow
using Base64
using DocStringExtensions
using DataFrames
using Dates
using DataStructures
using Distributed
using FileWatching
using HTTP
using JLD2
using JSON3
using JSONTables
using Logging
using MbedTLS
using Random
using Reexport
using Sockets
using Parameters
using PrecompileTools
using Preferences
using Printf
using Prometheus
using URIs
using Serialization
using UUIDs
@reexport using Visor
using ZMQ

export RembusError,
    RembusTimeout,
    RpcMethodNotFound,
    RpcMethodUnavailable,
    RpcMethodLoopback,
    RpcMethodException

export QOS0,
    QOS1,
    QOS2,
    authorize,
    unauthorize,
    get_private_topics,
    private_topic,
    public_topic,
    broker,
    component,
    server,
    connect,
    close,
    fpc,
    fdc,
    direct,
    rpc,
    publish,
    expose,
    unexpose,
    subscribe,
    unsubscribe,
    inject,
    reactive,
    unreactive,
    register,
    unregister,
    issuccess,
    tid,
    @component,
    @publish,
    @subscribe,
    @unsubscribe,
    @reactive,
    @unreactive,
    @rpc,
    @expose,
    @unexpose,
    @inject,
    @wait

include("constants.jl")
include("configuration.jl")
include("types.jl")
include("protocol.jl")
include("helpers.jl")
include("logger.jl")
include("cbor.jl")
include("decode.jl")
include("encode.jl")
include("twin.jl")
include("broker.jl")
include("admin.jl")
include("register.jl")
include("store.jl")
include("transport.jl")
include("api.jl")
include("http.jl")

function __init__()
    Visor.setroot(intensity=3)
    atexit(shutdown)
end

@setup_workload begin
    rembus_dir!("/tmp/rembus_compile")
    Rembus.zmq_ping_interval!(0)
    Rembus.ws_ping_interval!(0)
    Rembus.request_timeout!(20)
    @compile_workload begin
        rb = start_broker(
            authenticated=false,
            ws=8000,
            tcp=8001,
            zmq=8002,
            http=9000,
        )
        yield()
        islistening(rb, wait=20)
        include("precompile.jl")
        shutdown()
    end
    rembus_dir!(default_rembus_dir())
end

end
