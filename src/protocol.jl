abstract type RembusMsg end

@enum InOut pktin pktout
struct ProbedMsg
    ts::Libc.TimeVal
    direction::InOut
    msg::RembusMsg
end

const probeCollector = Dict{String,Vector{ProbedMsg}}()

abstract type RembusTopicMsg <: RembusMsg end

struct PingMsg <: RembusMsg
    id::UInt128
    cid::String
    twin::Twin
    PingMsg(twin::Twin, id::UInt128, cid::String) = new(id, cid, twin)
    PingMsg(twin::Twin, cid::String) = new(id(), cid, twin)
end

Base.show(io::IO, m::PingMsg) = show(io, "PING|$(m.id)|$(m.cid)")

struct IdentityMsg <: RembusMsg
    id::UInt128
    cid::String
    meta::Dict
    twin::Twin
    IdentityMsg(
        twin::Twin,
        cid::AbstractString,
        meta=Dict()
    ) = new(id(), cid, meta, twin)
    IdentityMsg(
        twin::Twin,
        msgid::UInt128,
        cid::AbstractString,
        meta=Dict()
    ) = new(msgid, cid, meta, twin)
end

Base.show(io::IO, m::IdentityMsg) = show(io, "IDY|$(m.id)|$(m.cid)|$(m.meta)")

mutable struct PubSubMsg{T} <: RembusTopicMsg
    topic::String
    data::T
    flags::UInt8
    id::UInt128
    twin::Twin
    counter::Int
    function PubSubMsg(twin::Twin, topic, data=nothing, flags=0x0, mid=0)
        if mid == 0 && flags > QOS0
            mid = id()
        end
        return new{typeof(data)}(topic, data, flags, mid, twin, 0)
    end
end

Base.show(io::IO, m::PubSubMsg) = show(io, "PUB|$(m.id)|$(m.topic)|$(m.flags)")

mutable struct RpcReqMsg{T} <: RembusTopicMsg
    id::UInt128
    topic::String
    data::T
    target::Union{Nothing,String}
    flags::UInt8
    twin::Twin

    function RpcReqMsg(twin::Twin, topic::AbstractString, data, target=nothing, flags=0x0)
        return new{typeof(data)}(id(), topic, data, target, flags, twin)
    end

    function RpcReqMsg(
        twin::Twin,
        msgid::UInt128,
        topic::AbstractString,
        data,
        target=nothing,
        flags=0x0
    )
        return new{typeof(data)}(msgid, topic, data, target, flags, twin)
    end
end

Base.show(io::IO, m::RpcReqMsg) = show(io, "RPC|$(m.id)|$(m.topic)|$(m.target)")

mutable struct AdminReqMsg{T} <: RembusTopicMsg
    id::UInt128
    topic::String
    data::T
    target::Union{Nothing,String}
    flags::UInt8
    twin::Twin

    function AdminReqMsg(twin::Twin, msgid::UInt128, topic, data, target=nothing, flags=0x0)
        return new{typeof(data)}(msgid, topic, data, target, flags, twin)
    end

    function AdminReqMsg(twin::Twin, topic::String, data, target=nothing, flags=0x0)
        return new{typeof(data)}(id(), topic, data, target, flags, twin)
    end
end

Base.show(io::IO, m::AdminReqMsg) = show(io, "ADM|$(m.id)|$(m.topic)|$(m.data)")

struct EnableReactiveMsg <: RembusMsg
    id::UInt128
    msg_from::Float64
end

struct AckMsg <: RembusMsg
    twin::Twin
    id::UInt128
end

Base.show(io::IO, m::AckMsg) = show(io, "ACK|$(m.id)")

struct Ack2Msg <: RembusMsg
    twin::Twin
    id::UInt128
end

Base.show(io::IO, m::Ack2Msg) = show(io, "ACK2|$(m.id)")

mutable struct ResMsg{T} <: RembusMsg
    id::UInt128
    status::UInt8
    data::T
    flags::UInt8
    twin::Twin
    reqdata::Any

    ResMsg(
        twin::Twin, id::UInt128, status, data, flags=0x0
    ) = new{typeof(data)}(id, status, data, flags, twin)

    function ResMsg(req::RpcReqMsg, status::UInt8, data=nothing, flags=0x0)
        return new{typeof(data)}(req.id, status, data, flags, req.twin)
    end

    function ResMsg(req::AdminReqMsg, status::UInt8, data=nothing, flags=0x0)
        return new{typeof(data)}(req.id, status, data, flags, req.twin)
    end
end

Base.show(io::IO, m::ResMsg) = show(io, "RES|$(m.id)|status:$(m.status)")

struct Register <: RembusMsg
    id::UInt128
    cid::String # client name
    pin::String
    pubkey::Vector{UInt8}
    type::UInt8
    twin::Twin
    Register(
        twin::Twin,
        msgid::UInt128,
        cid::AbstractString,
        pin::AbstractString,
        pubkey::Vector{UInt8},
        type::UInt8) = new(msgid, cid, pin, pubkey, type, twin)
end

Base.show(io::IO, m::Register) = show(io, "REG|$(m.id)|$(m.cid)")

struct Unregister <: RembusMsg
    id::UInt128
    twin::Twin
    Unregister(twin::Twin) = new(id(), twin)
    Unregister(twin::Twin, msgid::UInt128) = new(msgid, twin)
end

Base.show(io::IO, m::Unregister) = show(io, "UNREG|$(m.id)")

struct Attestation <: RembusMsg
    id::UInt128
    cid::String # client name
    signature::Vector{UInt8}
    meta::Dict
    twin::Twin
    function Attestation(
        twin::Twin, cid::AbstractString, signature::Vector{UInt8}, meta::Dict
    )
        return new(CONNECTION_ID, cid, signature, meta, twin)
    end

    function Attestation(
        twin::Twin, msgid::UInt128, cid::AbstractString, signature::Vector{UInt8}, meta::Dict
    )
        return new(msgid, cid, signature, meta, twin)
    end
end

Base.show(io::IO, m::Attestation) = show(io, "ATT|$(m.id)|$(m.cid)")

# Message for notifying the broker that the component is closing the socket.
# Apply to ZeroMQ protocol.
struct Close <: RembusMsg
    twin::Twin
    Close() = new()
    Close(twin) = new(twin)
end

function id()
    tv = Libc.TimeVal()
    UInt128(tv.sec * 1_000_000 + tv.usec) << 64 + (uuid4().value & 0xffffffffffffffff)
end

function response_timeout(twin, msg::RembusMsg)
    if haskey(twin.socket.direct, msg.id)
        condition = twin.socket.direct[msg.id].future
        if !isready(condition)
            put!(condition, RembusTimeout(msg))
        end
        delete!(twin.socket.direct, msg.id)
    end

    return nothing
end

function twin_future_request(twin::Twin, msg::RembusMsg, timeout)
    mid::UInt128 = msg.id
    timer = Timer((tim) -> response_timeout(twin, msg), timeout)
    resp_cond = FutureResponse(msg, timer)
    twin.socket.direct[mid] = resp_cond
    transport_send(twin, msg)
    return resp_cond
end

function twin_request(rb::Twin, msg::RembusMsg, timeout)
    resp_cond = twin_future_request(rb, msg, timeout)

    # fetch the full response message
    response = fetch(resp_cond.future)
    if isa(response, RembusTimeout)
        close(rb.socket)
        throw(response)
    else
        close(resp_cond.timer)
    end
    return response
end
