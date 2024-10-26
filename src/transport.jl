#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

const DATA_EMPTY = UInt8[0xf6]

const HEADER_LEN1 = 0x81
const HEADER_LEN2 = 0x8D
const HEADER_LEN4 = 0x8F

zmqsocketlock = ReentrantLock()

const MESSAGE_END = UInt8[0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef]

const WAIT_ID = UInt8(1)
const WAIT_EMPTY = UInt8(2)
const WAIT_HEADER = UInt8(3)
const WAIT_DATA = UInt8(4)

mutable struct ZMQPacket
    status::UInt8
    identity::Vector{UInt8}
    header::Vector{Any}
    data::ZMQ.Message
    ZMQPacket() = new(WAIT_ID, UInt8[], [], Message())
end

#=
    zmq_load(socket::ZMQ.Socket)

Get a Rembus message from a ZeroMQ multipart message.

The decoding is performed at the client side.
=#

function zmq_load(socket::ZMQ.Socket)
    pkt = zmq_message(socket)
    return zmq2msg(pkt)
end

function zmq2msg(pkt)
    header = pkt.header
    data::Vector{UInt8} = pkt.data

    type = header[1]
    ptype = type & 0x0f
    flags = type & 0xf0
    if ptype == TYPE_PUB
        if flags == QOS0
            ackid = 0
            topic = header[2]
        else
            ackid = bytes2id(header[2])
            topic = header[3]
        end
        msg = PubSubMsg(topic, dataframe_if_tagvalue(decode(data)), flags, ackid)
    elseif ptype == TYPE_RPC
        id = bytes2id(header[2])
        topic = header[3]
        target = header[4]
        msg = RpcReqMsg(id, topic, dataframe_if_tagvalue(decode(data)), target, flags)
    elseif ptype == TYPE_RESPONSE
        id = bytes2id(header[2])
        status = header[3]
        # NOTE: for very large dataframes decode is slow, needs investigation.
        val = decode(data)
        msg = ResMsg(id, status, dataframe_if_tagvalue(val), flags)
    elseif ptype == TYPE_ACK
        id = bytes2id(header[2])
        return AckMsg(id)
    elseif ptype == TYPE_ACK2
        id = bytes2id(header[2])
        return Ack2Msg(id)
    elseif ptype == TYPE_ADMIN
        id = bytes2id(header[2])
        topic = header[3]
        return AdminReqMsg(id, topic, decode(data))
    else
        throw(ErrorException("unknown packet type $ptype"))
    end

    return msg
end

#=
    from_cbor(packet)

Get a Rembus message decoding from a CBOR binary.

The decoding is performed at the component side.
=#
function from_cbor(packet)
    payload = decode(packet)
    ptype = payload[1] & 0x0f
    flags = payload[1] & 0xf0
    if ptype == TYPE_PUB
        if flags > QOS0
            ackid = bytes2id(payload[2])
            data = dataframe_if_tagvalue(payload[4])
            return PubSubMsg(payload[3], data, flags, ackid)
        else
            ackid = 0
            data = dataframe_if_tagvalue(payload[3])
            return PubSubMsg(payload[2], data, flags, ackid)
        end
    elseif ptype == TYPE_RPC
        data = dataframe_if_tagvalue(payload[5])
        #                       id               topic
        return RpcReqMsg(bytes2id(payload[2]), payload[3], data)
    elseif ptype == TYPE_RESPONSE
        data = dataframe_if_tagvalue(payload[4])
        #                          id        status
        return ResMsg(bytes2id(payload[2]), payload[3], data, flags)
    elseif ptype == TYPE_ACK
        return AckMsg(bytes2id(payload[2]))
    elseif ptype == TYPE_ACK2
        return Ack2Msg(bytes2id(payload[2]))
    elseif ptype == TYPE_IDENTITY
        cid = payload[3]
        @debug "<<message IDENTITY, cid: $cid)"
        return IdentityMsg(bytes2id(payload[2]), cid)
    elseif ptype == TYPE_ADMIN
        #                                          topic        data
        return AdminReqMsg(bytes2id(payload[2]), payload[3], payload[4])
    elseif ptype == TYPE_REGISTER
        #                           cid        tenant      pubkey,      type
        return Register(
            bytes2id(payload[2]), payload[3], payload[4], payload[5], payload[6]
        )
    elseif ptype == TYPE_UNREGISTER
        #                                         cid
        return Unregister(bytes2id(payload[2]), payload[3])
    elseif ptype == TYPE_ATTESTATION
        #                                         cid         signature
        return Attestation(bytes2id(payload[2]), payload[3], payload[4])
    end
end

#=
    broker_parse(pkt)

Get a Rembus message from a CBOR encoded packet.

The decoding is performed at the broker side.
=#
function broker_parse(pkt)
    io = IOBuffer(pkt)
    header = read(io, UInt8)
    if (header & TYPE_BITS_MASK) !== TYPE_4
        error("invalid rembus packet")
    end
    # the first byte is the packet type
    type = read(io, UInt8)
    if type === 0x18
        type = read(io, UInt8)
    end
    ptype = type & 0x0f
    flags = type & 0xf0

    if ptype == TYPE_IDENTITY
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        @debug "<<message IDENTITY, cid: $cid)"
        return IdentityMsg(bytes2id(id), cid)
    elseif ptype == TYPE_PUB
        if flags > QOS0
            id = decode_internal(io, Val(TYPE_2))
            topic = decode_internal(io, Val(TYPE_3))
            @debug "<<message PUB/ACK, topic: $topic"
            return PubSubMsg(topic, io, flags, bytes2id(id))
        else
            # do not decode dataframe, just pass through the broker
            topic = decode_internal(io, Val(TYPE_3))
            @debug "<<message PUB, topic: $topic"
            return PubSubMsg(topic, io, flags)
        end
    elseif ptype == TYPE_RPC
        id = decode_internal(io, Val(TYPE_2))
        topic = decode_internal(io, Val(TYPE_3))
        target = decode_internal(io)
        @debug "<<message RPC, id:$id, topic: $topic"
        return RpcReqMsg(bytes2id(id), topic, io, target, flags)
    elseif ptype == TYPE_ADMIN
        id = decode_internal(io, Val(TYPE_2))
        topic = decode_internal(io, Val(TYPE_3))
        data = decode_internal(io)
        @debug "<<message: ADMIN, topic: $topic, data:$data"
        return AdminReqMsg(bytes2id(id), topic, data, flags)
    elseif ptype == TYPE_RESPONSE
        id = decode_internal(io, Val(TYPE_2))
        status = decode_internal(io, Val(TYPE_0))
        # do not decode dataframe, just pass through the broker
        @debug "<<message: RESPONSE, id:$id, status: $status"
        return ResMsg(bytes2id(id), status, io, flags)
    elseif ptype == TYPE_ACK
        id = decode_internal(io, Val(TYPE_2))
        @debug "<<message ACK: id: $id)"
        return AckMsg(bytes2id(id))
    elseif ptype == TYPE_REGISTER
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        tenant = decode_internal(io)
        pubkey = decode_internal(io)
        type = decode_internal(io)
        @debug "<<message REGISTER, cid: $cid"
        return Register(bytes2id(id), cid, tenant, pubkey, type)
    elseif ptype == TYPE_UNREGISTER
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        @debug "<<message UNREGISTER, cid: $cid"
        return Unregister(bytes2id(id), cid)
    elseif ptype == TYPE_ATTESTATION
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        signature = decode_internal(io)
        @debug "<<message ATTESTATION, cid: $cid"
        return Attestation(bytes2id(id), cid, signature)
    end
    error("unknown rembus packet type $ptype ($pkt)")
end

@inline tobytes(socket::Socket) = Vector{UInt8}(ZMQ.recv(socket))

mutable struct ZMQDealerPacket
    header::Vector{Any}
    data::ZMQ.Message
end

#=
    zmq_message(socket::ZMQ.Socket)::ZMQDealerPacket

Receive a Multipart ZeroMQ message.

Return the packet header and data values extracted from a DEALER socket.
=#
function zmq_message(socket::ZMQ.Socket)::ZMQDealerPacket
    expect_empty = true
    while true
        # empty frame
        if expect_empty
            msg = ZMQ.recv(socket)
            if !isempty(msg)
                @error "ZMQ delear: expected empty message"
                continue
            end
        else
            expect_empty = true
        end

        bval = tobytes(socket)
        try
            header = decode(bval)
            data = recv(socket)
            msgend = tobytes(socket)
            if msgend != MESSAGE_END
                if isempty(msgend)
                    # data may be the empty message part of the next message
                    expect_empty = false
                end
                @error "ZMQ dealer: expected end of message"
                continue
            end
            return ZMQDealerPacket(header, data)

        catch e
            @error "ZMQ dealer: wrong header $bval ($e)"
            if isempty(bval)
                expect_empty = false
            end
        end
    end
end

function is_identity(msg)
    if length(msg) != 5 || msg[1] != 0
        @error "ZMQ: not identity msg, got: $msg"
        return false
    end

    return true
end

#=
    zmq_message(router::Router)::ZMQPacket

Receive a Multipart ZeroMQ message.

Return the packet identity, header and data values extracted from a ROUTER socket.
=#
function zmq_message(router::AbstractRouter, pkt::ZMQPacket)::Bool
    pkt_is_valid = false
    header::Vector{UInt8} = []
    while !pkt_is_valid
        msg = recv(router.zmqsocket)
        if pkt.status == WAIT_ID
            if length(msg) != 5 || msg[1] != 0
                @error "ZMQ router: expected identity, got: $msg"
            else
                pkt.status = WAIT_EMPTY
                pkt.identity = msg
            end
        elseif pkt.status == WAIT_EMPTY
            if !isempty(msg)
                @error "ZMQ router [$identity]: expected empty message"
                if is_identity(msg)
                    pkt.identity = msg
                else
                    pkt.identity = UInt8[]
                    pkt.status = WAIT_ID
                end
            else
                pkt.status = WAIT_HEADER
            end
        elseif pkt.status == WAIT_HEADER
            header = msg
            pkt.status = WAIT_DATA
        elseif pkt.status == WAIT_DATA
            data = msg
            msgend = recv(router.zmqsocket)
            if msgend != MESSAGE_END
                # search for identity message in header, data or msgend
                if is_identity(header)
                    if (isempty(data))
                        # detected another zmq message
                        pkt.identity = header
                        header = msgend
                    else
                        # unable to detect header message type
                        # restart searching from identity
                        pkt.status = WAIT_ID
                    end
                elseif is_identity(data)
                    if (isempty(msgend))
                        # detected another zmq message
                        pkt.status = WAIT_HEADER
                        pkt.identity = data
                    else
                        # unable to detect data message type
                        # restart searching from identity
                        pkt.status = WAIT_ID
                    end
                elseif is_identity(msgend)
                    pkt.status = WAIT_EMPTY
                else
                    # unable to detect data message type
                    # restart searching from identity
                    pkt.status = WAIT_ID
                end
            else
                pkt.status = WAIT_ID
                pkt.header = decode(header)
                pkt.data = data
                pkt_is_valid = true
            end
        end
    end
    return pkt_is_valid
end

#=
    broker_parse(router::Router, pkt::ZMQPacket)

The Broker parser of ZeroMQ messages.

`pkt` is the zeromq message decoded as `[identity, header, data]`.
=#
function broker_parse(router::AbstractRouter, pkt::ZMQPacket)
    id = pkt.identity
    type = pkt.header[1]

    ptype = type & 0x0f
    flags = type & 0xf0

    @debug "[zmq parse] from $id recv type $type"

    if ptype == TYPE_IDENTITY
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        if isempty(cid)
            error("empty cid")
        end
        return IdentityMsg(mid, cid)
    elseif ptype == TYPE_PUB
        if flags === QOS0
            ack_id = 0
            topic = pkt.header[2]
        else
            ack_id = bytes2id(pkt.header[2])
            topic = pkt.header[3]
        end
        data = pkt.data
        return PubSubMsg(topic, data, flags, ack_id)
    elseif ptype == TYPE_RPC
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        target = pkt.header[4]
        data = pkt.data
        return RpcReqMsg(mid, topic, data, target, flags)
    elseif ptype == TYPE_ADMIN
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        data = message2data(pkt.data)
        return AdminReqMsg(mid, topic, data, flags)
    elseif ptype == TYPE_RESPONSE
        mid = bytes2id(pkt.header[2])
        status = pkt.header[3]
        data = pkt.data
        return ResMsg(mid, status, data, flags)
    elseif ptype == TYPE_ACK
        mid = bytes2id(pkt.header[2])
        return AckMsg(mid)
    elseif ptype == TYPE_REGISTER
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        tenant = pkt.header[4]
        type = pkt.header[5]
        pubkey::Vector{UInt8} = pkt.data
        return Register(mid, cid, tenant, pubkey, type)
    elseif ptype == TYPE_UNREGISTER
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        return Unregister(mid, cid)
    elseif ptype == TYPE_ATTESTATION
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        signature::Vector{UInt8} = pkt.data
        return Attestation(mid, cid, signature)
    elseif ptype == TYPE_PING
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        @debug "ping from [$cid]"
        return PingMsg(mid, cid)
        #    elseif ptype == TYPE_REMOVE
        #        return Remove()
    elseif ptype == TYPE_CLOSE
        return Close()
    end
    error("unknown rembus packet type $ptype")
end

function pong(socket, mid, identity)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_RESPONSE, id2bytes(mid), STS_SUCCESS]), more=true)
    send(socket, encode(nothing), more=true)
    send(socket, MESSAGE_END, more=false)
end

#=
data2payload(data) = data

function data2payload(data::IOBuffer)
    mark(data)
    decode(read(data))
    reset(data)
end
=#

function id2bytes(id::UInt128)::Vector{UInt8}
    io = IOBuffer(maxsize=16)
    write(io, id)
    take!(io)
end

function bytes2id(buff::Vector{UInt8})
    read(IOBuffer(buff), UInt128)
end

function bytes2zid(buff::Vector{UInt8})
    UInt128(read(IOBuffer(buff[2:end]), UInt32))
end

function transport_send(::Val{zdealer}, rb, msg::PingMsg)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_PING, id2bytes(msg.id), msg.cid]), more=true)
        send(rb.socket, DATA_EMPTY, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb, msg::IdentityMsg)
    pkt = [TYPE_IDENTITY, id2bytes(msg.id), msg.cid]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb, msg::IdentityMsg)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_IDENTITY, id2bytes(msg.id), msg.cid]), more=true)
        send(rb.socket, DATA_EMPTY, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

message2data(data) = data

# Return data to be sent via ws or tcp from ZMQ
function message2data(data::ZMQ.Message)
    # allocate instead of consuming message
    decode(Vector{UInt8}(data))
end

# Return a ZMQ message object from data received from ws or tcp sockets.
data2message(data) = Message(encode(data))

function data2message(data::IOBuffer)
    pos = position(data)
    msg = Message(read(data))
    seek(data, pos)
    return msg
end

# allocate and return a buffer because send method consume the Message
# needed by multiple broadcast invocations (reqdata field).
data2message(data::ZMQ.Message) = Vector{UInt8}(data)

transport_send(twin, ::Nothing, msg) = error("$twin connection closed")

function transport_send(
    ::Val{socket},
    twin::Twin,
    msg::PubSubMsg
)
    outcome = true
    if msg.flags > QOS0
        msgid = msg.id

        ack_cond = Distributed.Future()
        twin.out[msgid] = ack_cond
        twin.acktimer[msgid] = Timer((tim) -> handle_ack_timeout(
                tim, twin, msg, msgid
            ), ACK_WAIT_TIME)
        pkt = [TYPE_PUB | msg.flags, id2bytes(msgid), msg.topic, msg.data]
        broker_transport_write(twin.socket, pkt)
        outcome = fetch(ack_cond)
        delete!(twin.out, msgid)
    else
        pkt = [TYPE_PUB | msg.flags, msg.topic, msg.data]
        broker_transport_write(twin.socket, pkt)
    end

    return outcome
end

#=
    client_ack_timeout(tim, twin, msg, msgid)

Resend a PubSub message in case the acknowledge message is not received.
=#
function client_ack_timeout(tim, rb, msg, msgid)
    # TODO: is isopen() control needed?
    if haskey(rb.acktimer, msgid) & isopen(rb.process.inbox)
        put!(rb.process.inbox, msg)
    end

    if haskey(rb.out, msgid)
        put!(rb.out[msgid], false)
    end
end

function transport_send(::Val{socket}, rb::RBHandle, msg::PubSubMsg)
    content = tagvalue_if_dataframe(msg.data)
    outcome = true
    if msg.flags > QOS0
        msgid = msg.id
        ack_cond = Distributed.Future()
        rb.out[msgid] = ack_cond
        rb.acktimer[msgid] = Timer((tim) -> client_ack_timeout(
                tim, rb, msg, msgid
            ), ACK_WAIT_TIME)
        pkt = [TYPE_PUB | msg.flags, id2bytes(msgid), msg.topic, content]
        transport_write(rb.socket, pkt)
        outcome = fetch(ack_cond)
        close(rb.acktimer[msgid])
        delete!(rb.acktimer, msgid)
        delete!(rb.out, msgid)
    else
        pkt = [TYPE_PUB | msg.flags, msg.topic, content]
        transport_write(rb.socket, pkt)
    end

    return outcome
end

function transport_send(::Val{zdealer}, rb::RBConnection, msg::PubSubMsg)
    content = tagvalue_if_dataframe(msg.data)
    outcome = true
    if msg.flags == QOS0
        lock(zmqsocketlock) do
            send(rb.socket, Message(), more=true)
            send(rb.socket, encode([TYPE_PUB | msg.flags, msg.topic]), more=true)
            send(rb.socket, encode(content), more=true)
            send(rb.socket, MESSAGE_END, more=false)
        end
    else
        msgid = msg.id
        ack_cond = Distributed.Future()
        rb.out[msgid] = ack_cond
        rb.acktimer[msgid] = Timer((tim) -> client_ack_timeout(
                tim, rb, msg, msgid
            ), ACK_WAIT_TIME)
        lock(zmqsocketlock) do
            send(rb.socket, Message(), more=true)
            send(rb.socket, encode([TYPE_PUB | msg.flags, id2bytes(msg.id), msg.topic]), more=true)
            send(rb.socket, encode(content), more=true)
            send(rb.socket, MESSAGE_END, more=false)
        end
        outcome = fetch(ack_cond)
        close(rb.acktimer[msgid])
        delete!(rb.acktimer, msgid)
        delete!(rb.out, msgid)
    end
    return outcome
end

function transport_send(::Val{zrouter}, twin, msg::PubSubMsg)
    address = twin.router.twin2address[twin.id]
    data = data2message(msg.data)
    outcome = true
    if msg.flags > QOS0
        ack_cond = Distributed.Future()
        twin.out[msg.id] = ack_cond

        twin.acktimer[msg.id] = Timer((tim) -> handle_ack_timeout(
                tim, twin, msg, msg.id
            ), ACK_WAIT_TIME)
        header = encode([TYPE_PUB | msg.flags, id2bytes(msg.id), msg.topic])
        lock(zmqsocketlock) do
            send(twin.socket, address, more=true)
            send(twin.socket, Message(), more=true)
            send(twin.socket, header, more=true)
            send(twin.socket, data, more=true)
            send(twin.socket, MESSAGE_END, more=false)
        end

        outcome = fetch(ack_cond)
        delete!(twin.out, msg.id)
    else
        header = encode([TYPE_PUB | msg.flags, msg.topic])
        lock(zmqsocketlock) do
            send(twin.socket, address, more=true)
            send(twin.socket, Message(), more=true)
            send(twin.socket, header, more=true)
            send(twin.socket, data, more=true)
            send(twin.socket, MESSAGE_END, more=false)
        end
    end

    return outcome
end

function transport_send(::Val{socket}, rb::Twin, msg::RpcReqMsg)
    pkt = [
        TYPE_RPC | msg.flags,
        id2bytes(msg.id),
        msg.topic,
        msg.target,
        msg.data
    ]
    broker_transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{socket}, rb::RBHandle, msg::RpcReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    if msg.target === nothing
        pkt = [
            TYPE_RPC | msg.flags,
            id2bytes(msg.id),
            msg.topic,
            nothing,
            message2data(content)
        ]
    else
        pkt = [
            TYPE_RPC | msg.flags,
            id2bytes(msg.id),
            msg.topic,
            msg.target,
            message2data(content)
        ]
    end
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zrouter}, rb, msg::RpcReqMsg)
    address = rb.router.twin2address[rb.id]
    lock(zmqsocketlock) do
        send(rb.socket, address, more=true)
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, nothing]), more=true)
        send(rb.socket, data2message(msg.data), more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end

    return true
end

function transport_send(::Val{zdealer}, rb, msg::RpcReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)

        if msg.target === nothing
            target = nothing
        else
            target = msg.target
        end

        send(rb.socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, target]), more=true)
        send(rb.socket, encode(content), more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{zrouter}, twin, msg::ResMsg, enc=false)
    address = twin.zaddress
    lock(zmqsocketlock) do
        send(twin.socket, address, more=true)
        send(twin.socket, Message(), more=true)
        send(twin.socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
        if enc
            data = encode(msg.data)
        else
            data = data2message(msg.data)
        end
        send(twin.socket, data, more=true)
        send(twin.socket, MESSAGE_END, more=false)
    end

    return true
end

function transport_send(::Val{zdealer}, rb::RBConnection, msg::ResMsg)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
        send(rb.socket, encode(msg.data), more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb::Twin, msg::ResMsg, enc=false)
    pkt = [
        TYPE_RESPONSE | msg.flags,
        id2bytes(msg.id),
        msg.status,
        tagvalue_if_dataframe(msg.data)
    ]
    broker_transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{loopback}, rb::Twin, msg::ResMsg, enc=false)
    notify(rb.socket, msg)
    return true
end

function transport_send(::Val{socket}, rb::RBHandle, msg::ResMsg, ::Bool=false)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_RESPONSE | msg.flags, id2bytes(msg.id), msg.status, message2data(content)]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{socket}, rb::RBHandle, msg::AdminReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_ADMIN | msg.flags, id2bytes(msg.id), msg.topic, content]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb::RBConnection, msg::AdminReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_ADMIN, id2bytes(msg.id), msg.topic]), more=true)
        send(rb.socket, encode(content), more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{zrouter}, rb, msg::AdminReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    address = rb.zaddress
    lock(zmqsocketlock) do
        send(rb.socket, address, more=true)
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_ADMIN, id2bytes(msg.id), msg.topic]), more=true)
        send(rb.socket, encode(content), more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb::Twin, msg::AckMsg)
    pkt = [TYPE_ACK, id2bytes(msg.id)]
    broker_transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zrouter}, twin, msg::AckMsg)
    address = twin.zaddress
    lock(zmqsocketlock) do
        send(twin.socket, address, more=true)
        send(twin.socket, Message(), more=true)
        send(twin.socket, encode([TYPE_ACK, id2bytes(msg.id)]), more=true)
        send(twin.socket, DATA_EMPTY, more=true)
        send(twin.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb::Twin, msg::Ack2Msg)
    pkt = [TYPE_ACK2, id2bytes(msg.id)]
    broker_transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zrouter}, twin, msg::Ack2Msg)
    address = twin.zaddress
    lock(zmqsocketlock) do
        send(twin.socket, address, more=true)
        send(twin.socket, Message(), more=true)
        send(twin.socket, encode([TYPE_ACK2, id2bytes(msg.id)]), more=true)
        send(twin.socket, DATA_EMPTY, more=true)
        send(twin.socket, MESSAGE_END, more=false)
    end
    return true
end

#=
The twin send an admin request to the peer broker.
=#
function transport_send(::Val{socket}, rb, msg::AdminReqMsg)
    pkt = [TYPE_ADMIN | msg.flags, id2bytes(msg.id), msg.topic, msg.data]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{socket}, rb::RBHandle, msg::AckMsg)
    pkt = [TYPE_ACK, id2bytes(msg.id)]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb, msg::AckMsg)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_ACK, id2bytes(msg.id)]), more=true)
        send(rb.socket, DATA_EMPTY, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb, msg::Attestation)
    pkt = [TYPE_ATTESTATION, id2bytes(msg.id), msg.cid, msg.signature]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb, msg::Attestation)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_ATTESTATION, id2bytes(msg.id), msg.cid]), more=true)
        send(rb.socket, msg.signature, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb, msg::Register)
    pkt = [TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.tenant, msg.pubkey, msg.type]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb, msg::Register)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(
            rb.socket,
            encode([TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.tenant, msg.type]),
            more=true
        )
        send(rb.socket, msg.pubkey, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{socket}, rb, msg::Unregister)
    pkt = [TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]
    transport_write(rb.socket, pkt)
    return true
end

function transport_send(::Val{zdealer}, rb, msg::Unregister)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]), more=true)
        send(rb.socket, DATA_EMPTY, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(::Val{zdealer}, rb, ::Close)
    lock(zmqsocketlock) do
        send(rb.socket, Message(), more=true)
        send(rb.socket, encode([TYPE_CLOSE]), more=true)
        send(rb.socket, DATA_EMPTY, more=true)
        send(rb.socket, MESSAGE_END, more=false)
    end
    return true
end

function tagvalue_if_dataframe(data)
    if isa(data, Vector{UInt8})
        return data
    elseif isa(data, Vector)
        result = []

        for el in data
            if isa(el, DataFrame)
                aio = IOBuffer()
                Arrow.write(aio, el)
                push!(result, Tag(DATAFRAME_TAG, aio.data))
            else
                push!(result, el)
            end
        end
        return result
    elseif isa(data, DataFrame)
        aio = IOBuffer()
        Arrow.write(aio, data)

        return Tag(DATAFRAME_TAG, aio.data)
    end

    return data
end

function dataframe_if_tagvalue(buffer)
    if isa(buffer, Vector)
        result = []

        for el in buffer
            if isa(el, Tag) && el.id == DATAFRAME_TAG
                @debug "assuming that element is a DataFrame"
                push!(result, DataFrame(Arrow.Table(IOBuffer(el.data))))
            else
                push!(result, el)
            end
        end
        return result
    else
        if isa(buffer, Tag) && buffer.id == DATAFRAME_TAG
            @debug "assuming that data is a DataFrame"
            return DataFrame(Arrow.Table(IOBuffer(buffer.data)))
        end
    end

    return buffer
end

function ws_write(ws::WebSockets.WebSocket, payload)
    @rawlog("out: $payload")
    HTTP.WebSockets.send(ws, payload)
end

function broker_transport_write(::Nothing, pkt)
    @warn "broker send $pkt failed: connection closed"
end

function broker_transport_write(ws::WebSockets.WebSocket, pkt)
    payload = encode_partial(pkt)
    ws_write(ws, payload)
end

function transport_write(::Nothing, pkt)
    @warn "send $pkt failed: connection closed"
end

function transport_write(ws::WebSockets.WebSocket, pkt)
    payload = encode(pkt)
    ws_write(ws, payload)
end

function tcp_write(sock, payload)
    len = length(payload)
    if len < 256
        header = vcat(HEADER_LEN1, UInt8(len))
    elseif len < 65536
        header = vcat(HEADER_LEN2, UInt8((len >> 8) & 0xFF), UInt8(len & 0xFF))
    else
        header = vcat(
            HEADER_LEN4,
            UInt8((len >> 24) & 0xFF),
            UInt8((len >> 16) & 0xFF),
            UInt8((len >> 8) & 0xFF),
            UInt8(len & 0xFF)
        )
    end

    io = IOBuffer(maxsize=length(header) + len)
    write(io, header)
    write(io, payload)
    @rawlog("out: $(io.data)")
    write(sock, io.data)
    flush(sock)
end

function broker_transport_write(sock, llmsg)
    payload = encode_partial(llmsg)
    tcp_write(sock, payload)
end

function transport_write(sock, llmsg)
    payload = encode(llmsg)
    tcp_write(sock, payload)
end

function transport_read(sock)
    headers = read(sock, 1)
    if isempty(headers)
        return headers # connection closed
    end
    type = headers[1]
    if type === HEADER_LEN1
        len = read(sock, 1)[1]
    elseif type === HEADER_LEN2
        lb = read(sock, 2)
        len = Int(lb[1]) << 8 + lb[2]
    elseif type === HEADER_LEN4
        lb = read(sock, 4)
        len = Int(lb[1]) << 24 + Int(lb[2]) << 16 + Int(lb[3]) << 8 + lb[4]
    else
        @error "tcp channel invalid header value [$type]"
        throw(WrongTcpPacket())
    end
    payload = read(sock, len)
    @rawlog("in: $payload")
    return payload
end

function transport_read(socket::WebSockets.WebSocket)
    d = HTTP.WebSockets.receive(socket)
    #@info "IN: $d"
    @rawlog("in: $d ($(typeof(d)))")
    return d
end

function isconnectionerror(ws::WebSockets.WebSocket, e)
    isa(e, EOFError) ||
        isa(e, Base.IOError) ||
        isa(e, WebSockets.WebSocketError)
end

function isconnectionerror(ws, e)
    return isa(e, EOFError) || isa(e, Base.IOError) || isa(e, WrongTcpPacket)
end

Base.isopen(ws::WebSockets.WebSocket) = Base.isopen(ws.io)
