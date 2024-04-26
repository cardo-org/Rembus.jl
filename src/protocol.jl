abstract type RembusMsg end

struct PingMsg <: RembusMsg
    id::UInt128
    cid::String
    PingMsg(id::UInt128, cid::String) = new(id, cid)
    PingMsg(cid::String) = new(id(), cid)
end

struct IdentityMsg <: RembusMsg
    id::UInt128
    cid::String
    IdentityMsg(userid::AbstractString) = new(id(), userid)
    IdentityMsg(msgid::UInt128, userid::AbstractString) = new(msgid, userid)
end

mutable struct PubSubMsg{T} <: RembusMsg
    topic::String
    data::T
    flags::UInt8
    hash::UInt128

    function PubSubMsg(topic, data=nothing, flags=0x0, hash=0)
        return new{typeof(data)}(topic, data, flags, hash)
    end
end

Base.show(io::IO, message::PubSubMsg) = show(io, message.topic)

struct RpcReqMsg{T} <: RembusMsg
    id::UInt128
    topic::String
    data::T
    target::Union{Nothing,String}
    flags::UInt8

    function RpcReqMsg(topic::AbstractString, data, target=nothing, flags=0x0)
        return new{typeof(data)}(id(), topic, data, target, flags)
    end

    function RpcReqMsg(
        msgid::UInt128,
        topic::AbstractString,
        data,
        target=nothing,
        flags=0x0
    )
        return new{typeof(data)}(msgid, topic, data, target, flags)
    end
end

Base.show(io::IO, message::RpcReqMsg) = show(io, message.topic)

struct AdminReqMsg{T} <: RembusMsg
    id::UInt128
    topic::String
    data::T
    flags::UInt8

    function AdminReqMsg(msgid::UInt128, topic, data, flags=0x0)
        return new{typeof(data)}(msgid, topic, data, flags)
    end

    function AdminReqMsg(topic::String, data, flags=0x0)
        return new{typeof(data)}(id(), topic, data, flags)
    end
end

struct AckMsg <: RembusMsg
    hash::UInt128
end

struct ResMsg{T} <: RembusMsg
    id::UInt128
    status::UInt8
    data::T
    flags::UInt8

    ResMsg(id, status, data, flags=0x0) = new{typeof(data)}(id, status, data, flags)

    function ResMsg(req::RpcReqMsg, status::UInt8, data=nothing, flags=0x0)
        return new{typeof(data)}(req.id, status, data, flags)
    end

    function ResMsg(exec::RpcReqMsg, flags=0x0)
        return new{typeof(exec.data)}(exec.id, exec.status, exec.data, flags)
    end
end

Base.show(io::IO, message::ResMsg) = show(io, "msgid:$(message.id) status:$(message.status)")

struct Register <: RembusMsg
    id::UInt128
    cid::String # client name
    userid::String
    pubkey::Vector{UInt8}
    Register(
        msgid::UInt128,
        cid::AbstractString,
        userid::AbstractString,
        pubkey::Vector{UInt8}) = new(msgid, cid, userid, pubkey)
end

struct Unregister <: RembusMsg
    id::UInt128
    cid::String # client name
    Unregister(cid::String) = new(id(), cid)
    Unregister(msgid::UInt128, cid::String) = new(msgid, cid)
end

struct Attestation <: RembusMsg
    id::UInt128
    cid::String # client name
    signature::Vector{UInt8}

    Attestation(cid::AbstractString, signature::Vector{UInt8}) = new(id(), cid, signature)

    function Attestation(msgid::UInt128, cid::AbstractString, signature::Vector{UInt8})
        return new(msgid, cid, signature)
    end
end

# Message for notifying the broker that the component is closing the socket.
# Apply to ZeroMQ protocol.
struct Close <: RembusMsg
end

# Remove the twin of a component from the broker.
# Apply to ZeroMQ protocol.
struct Remove <: RembusMsg
end

#id() = uuid4().value  # unique message identifier

counter = UInt128(0)
function id()
    global counter
    counter += 1
    return counter
end

isresponse(msg::RembusMsg) = false
isresponse(msg::ResMsg) = true
