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
using TaskLocalValues
using UUIDs
@reexport using Visor
using ZMQ

export RembusError,
    RembusTimeout,
    RpcMethodNotFound,
    RpcMethodUnavailable,
    RpcMethodLoopback,
    RpcMethodException

export
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
    request_timeout,
    request_timeout!,
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
    warn!()
    ENV["REMBUS_DIR"] = joinpath(tempdir(), "rembus_compile")
    ENV["REMBUS_TIMEOUT"] = "20"
    ENV["REMBUS_ZMQ_PING_INTERVAL"] = "0"
    ENV["REMBUS_WS_PING_INTERVAL"] = "0"
    @compile_workload begin
        rb = get_router(
            name="broker",
            ws=8000,
            tcp=8001,
            zmq=8002,
            http=9000,
            authenticated=false,
        )
        yield()
        islistening(rb, wait=20)
        include("precompile.jl")
        shutdown()
    end
end

end
