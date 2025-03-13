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

abstract type ZMQAbstractPacket end

mutable struct ZMQPacket <: ZMQAbstractPacket
    identity::Vector{UInt8}
    header::Vector{Any}
    data::ZMQ.Message
end

mutable struct ZMQDealerPacket <: ZMQAbstractPacket
    header::Vector{Any}
    data::ZMQ.Message
end

#=
    zmq_load(twin::Twin, pkt, socket::ZMQ.Socket)

Get a Rembus message from a ZeroMQ multipart message.
=#
function zmq_load(twin::Twin, socket::ZMQ.Socket)
    pkt = zmq_message(socket)
    return zmq_parse(twin, pkt, false)
end

#=
    from_cbor(twin, msgid, counter, packet)

Send a Rembus message from a CBOR encoded array of bytes.

Currently only a PubSub message is supported.
=#
function from_cbor(twin, counter, packet)
    payload = decode(packet)
    ptype = payload[1] & 0x0f
    flags = payload[1] & 0xf0
    if ptype == TYPE_PUB
        if flags > QOS0
            ackid = bytes2id(payload[2])
            data = dataframe_if_tagvalue(payload[4])
            msg = PubSubMsg(twin, payload[3], data, flags, ackid)
        else
            ackid = 0
            data = dataframe_if_tagvalue(payload[3])
            msg = PubSubMsg(twin, payload[2], data, flags, ackid)
        end
        msg.counter = counter
        put!(twin.process.inbox, msg)
    end

    return nothing
end

#=
    broker_parse(pkt)

Get a Rembus message from a CBOR packet.
=#
function broker_parse(twin::Twin, pkt)
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
        meta = decode_internal(io)
        return IdentityMsg(twin, bytes2id(id), cid, meta)
    elseif ptype == TYPE_PUB
        if flags > QOS0
            id = decode_internal(io, Val(TYPE_2))
            topic = decode_internal(io, Val(TYPE_3))
            return PubSubMsg(twin, topic, io, flags, bytes2id(id))
        else
            # Do not decode dataframe, just pass through the broker.
            topic = decode_internal(io, Val(TYPE_3))
            return PubSubMsg(twin, topic, io, flags)
        end
    elseif ptype == TYPE_RPC
        id = decode_internal(io, Val(TYPE_2))
        topic = decode_internal(io, Val(TYPE_3))
        target = decode_internal(io)
        return RpcReqMsg(twin, bytes2id(id), topic, io, target, flags)
    elseif ptype == TYPE_ADMIN
        id = decode_internal(io, Val(TYPE_2))
        topic = decode_internal(io, Val(TYPE_3))
        data = decode_internal(io)
        return AdminReqMsg(twin, bytes2id(id), topic, Dict{String,Any}(data), nothing, flags)
    elseif ptype == TYPE_RESPONSE
        id = decode_internal(io, Val(TYPE_2))
        status = decode_internal(io, Val(TYPE_0))
        # Do not decode dataframe, just pass through the broker.
        return ResMsg(twin, bytes2id(id), status, io, flags)
    elseif ptype == TYPE_ACK
        id = decode_internal(io, Val(TYPE_2))
        return AckMsg(twin, bytes2id(id))
    elseif ptype == TYPE_ACK2
        id = decode_internal(io, Val(TYPE_2))
        return Ack2Msg(twin, bytes2id(id))
    elseif ptype == TYPE_REGISTER
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        tenant = decode_internal(io)
        pubkey = decode_internal(io)
        type = decode_internal(io)
        return Register(twin, bytes2id(id), cid, tenant, pubkey, type)
    elseif ptype == TYPE_UNREGISTER
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        return Unregister(twin, bytes2id(id), cid)
    elseif ptype == TYPE_ATTESTATION
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        signature = decode_internal(io)
        meta = decode_internal(io)
        return Attestation(twin, bytes2id(id), cid, signature, meta)
    end
    error("unknown rembus packet type $ptype ($pkt)")
end

@inline tobytes(socket::Socket) = Vector{UInt8}(ZMQ.recv(socket))

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
function zmq_message(router::Router)::ZMQPacket
    pkt_is_valid = false
    status = WAIT_ID
    header = UInt8[]
    identy = UInt8[]
    data = Message()
    while !pkt_is_valid
        msg = recv(router.zmqsocket)
        if status == WAIT_ID
            if length(msg) != 5 || msg[1] != 0
                @error "[ZMQ router] expected identity, got: $msg"
            else
                status = WAIT_EMPTY
                identy = msg
            end
        elseif status == WAIT_EMPTY
            if !isempty(msg)
                @error "[ZMQ router] expected empty message, got: $msg"
                if is_identity(msg)
                    identy = msg
                else
                    identy = UInt8[]
                    status = WAIT_ID
                end
            else
                status = WAIT_HEADER
            end
        elseif status == WAIT_HEADER
            header = msg
            status = WAIT_DATA
        elseif status == WAIT_DATA
            data = msg
            msgend = recv(router.zmqsocket)
            if msgend != MESSAGE_END
                # Search for identity message in header, data or msgend.
                if is_identity(header)
                    if (isempty(data))
                        # Detected another zmq message.
                        identy = header
                        header = msgend
                    else
                        # Unable to detect header message type,
                        # restart searching from identity.
                        status = WAIT_ID
                    end
                elseif is_identity(data)
                    if (isempty(msgend))
                        # Detected another zmq message.
                        status = WAIT_HEADER
                        identy = data
                    else
                        # Unable to detect data message type,
                        # restart searching from identity
                        status = WAIT_ID
                    end
                elseif is_identity(msgend)
                    status = WAIT_EMPTY
                else
                    # Unable to detect data message type,
                    # restart searching from identity
                    status = WAIT_ID
                end
            else
                pkt_is_valid = true
            end
        end
    end
    return ZMQPacket(identy, decode(header), data)
end

#=
    zmq_parse(pkt::ZMQPacket, isbroker=true)

The parser of ZeroMQ messages.

`pkt` is the zeromq message decoded as `[identity, header, data]`.
=#
function zmq_parse(twin::Twin, pkt::ZMQAbstractPacket, isbroker=true)
    type = pkt.header[1]
    ptype = type & 0x0f
    flags = type & 0xf0

    if ptype == TYPE_IDENTITY
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        if length(pkt.header) == 4
            meta = pkt.header[4]
        else
            meta = Dict()
        end
        if isempty(cid)
            error("empty cid")
        end
        return IdentityMsg(twin, mid, cid, meta)
    elseif ptype == TYPE_PUB
        if flags === QOS0
            ack_id = 0
            topic = pkt.header[2]
        else
            ack_id = bytes2id(pkt.header[2])
            topic = pkt.header[3]
        end
        data = pkt.data
        if isbroker
            return PubSubMsg(
                twin, topic, data, flags, ack_id
            )
        else
            return PubSubMsg(
                twin, topic, dataframe_if_tagvalue(decode(data)), flags, ack_id
            )
        end
    elseif ptype == TYPE_RPC
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        target = pkt.header[4]
        data = pkt.data
        if isbroker
            return RpcReqMsg(twin, mid, topic, data, target, flags)
        else
            return RpcReqMsg(
                twin, mid, topic, dataframe_if_tagvalue(decode(data)), target, flags
            )
        end
    elseif ptype == TYPE_ADMIN
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        data = message2data(pkt.data)
        return AdminReqMsg(twin, mid, topic, Dict{String,Any}(data), nothing, flags)
    elseif ptype == TYPE_RESPONSE
        mid = bytes2id(pkt.header[2])
        status = pkt.header[3]
        data = pkt.data
        if isbroker
            return ResMsg(twin, mid, status, data, flags)
        else
            return ResMsg(twin, mid, status, dataframe_if_tagvalue(decode(data)), flags)
        end
    elseif ptype == TYPE_ACK
        mid = bytes2id(pkt.header[2])
        return AckMsg(twin, mid)
    elseif ptype == TYPE_ACK2
        mid = bytes2id(pkt.header[2])
        return Ack2Msg(twin, mid)
    elseif ptype == TYPE_REGISTER
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        tenant = pkt.header[4]
        type = pkt.header[5]
        pubkey::Vector{UInt8} = pkt.data
        return Register(twin, mid, cid, tenant, pubkey, type)
    elseif ptype == TYPE_UNREGISTER
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        return Unregister(twin, mid, cid)
    elseif ptype == TYPE_ATTESTATION
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        meta = pkt.header[4]
        signature::Vector{UInt8} = pkt.data
        return Attestation(twin, mid, cid, signature, meta)
    elseif ptype == TYPE_PING
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        @debug "ping from [$cid]"
        return PingMsg(twin, mid, cid)
        #    elseif ptype == TYPE_REMOVE
        #        return Remove()
    elseif ptype == TYPE_CLOSE
        return Close(twin)
    end
    error("unknown rembus packet type $ptype")
end

function pong(z::ZRouter, mid, identity)
    socket = z.sock
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_RESPONSE, id2bytes(mid), STS_SUCCESS]), more=true)
    send(socket, encode(nothing), more=true)
    send(socket, MESSAGE_END, more=false)
end

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

transport_send(::Float, ::RembusMsg) = true

function transport_send(twin::Twin, msg)
    twin.probe && probe_add(twin, msg, pktout)
    return transport_send(twin.socket, msg)
end

message_send(twin, future::FutureResponse) = transport_send(twin, future.request)

function message_send(twin, msg)
    if isa(msg, RpcReqMsg) || isa(msg, AdminReqMsg)
        router = last_downstream(twin.router)
        tmr = Timer(router.settings.request_timeout) do tmr
            delete!(twin.socket.out, msg.id)
        end
        twin.socket.out[msg.id] = FutureResponse(msg, tmr)
    end

    outcome = transport_send(twin, msg)
    if isa(msg, PubSubMsg) && outcome
        twin.mark = msg.counter
    end

    return outcome
end

function transport_send(z::ZDealer, msg::PingMsg)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_PING, id2bytes(msg.id), msg.cid]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::IdentityMsg)
    pkt = [TYPE_IDENTITY, id2bytes(msg.id), msg.cid, msg.meta]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZDealer, msg::IdentityMsg)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(
            socket,
            encode([TYPE_IDENTITY, id2bytes(msg.id), msg.cid, msg.meta]),
            more=true
        )
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

message2data(data) = data

# Return data to be sent via ws or tcp from ZMQ.
function message2data(data::ZMQ.Message)
    # Allocate instead of consuming message.
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

# Allocate and return a buffer because send method consume the message
# used by multiple broadcast invocations (reqdata field).
data2message(data::ZMQ.Message) = Vector{UInt8}(data)

#=
    handle_ack_timeout(tim, twin, msg, msgid)

Persist a PubSub message in case the acknowledge message is not received.
=#
function handle_ack_timeout(tim, socket, msg, msgid)
    if haskey(socket.out, msgid) && !isready(socket.out[msgid].future)
        put!(socket.out[msgid].future, false)
    end
end

function transport_send(socket::AbstractPlainSocket, msg::PubSubMsg)
    outcome = true
    if msg.flags > QOS0
        msgid = msg.id

        timer = Timer(
            (tim) -> handle_ack_timeout(tim, socket, msg, msgid),
            msg.twin.router.settings.ack_timeout
        )

        ack_cond = FutureResponse(msg, timer)
        socket.out[msgid] = ack_cond
        pkt = [TYPE_PUB | msg.flags, id2bytes(msgid), msg.topic, msg.data]
        transport_write(socket, pkt)
        outcome = fetch(ack_cond.future)
        close(ack_cond.timer)
        delete!(socket.out, msgid)
    else
        pkt = [TYPE_PUB | msg.flags, msg.topic, msg.data]
        transport_write(socket, pkt)
    end

    return outcome
end

function transport_send(z::ZDealer, msg::PubSubMsg)
    return transport_zmq_pubsub(z, msg)
end

function transport_send(z::ZRouter, msg::PubSubMsg)
    return transport_zmq_pubsub(z, msg)
end

function transport_zmq_pubsub(z, msg::PubSubMsg)
    socket = z.sock
    data = data2message(msg.data)
    outcome = true
    if msg.flags > QOS0
        timer = Timer(
            (tim) -> handle_ack_timeout(tim, z, msg, msg.id),
            msg.twin.router.settings.ack_timeout
        )
        ack_cond = FutureResponse(msg, timer)
        z.out[msg.id] = ack_cond
        header = encode([TYPE_PUB | msg.flags, id2bytes(msg.id), msg.topic])
        lock(zmqsocketlock) do
            isa(z, ZRouter) && send(socket, z.zaddress, more=true)
            send(socket, Message(), more=true)
            send(socket, header, more=true)
            send(socket, data, more=true)
            send(socket, MESSAGE_END, more=false)
        end
        outcome = fetch(ack_cond.future)
        delete!(z.out, msg.id)
    else
        header = encode([TYPE_PUB | msg.flags, msg.topic])
        lock(zmqsocketlock) do
            isa(z, ZRouter) && send(socket, z.zaddress, more=true)
            send(socket, Message(), more=true)
            send(socket, header, more=true)
            send(socket, data, more=true)
            send(socket, MESSAGE_END, more=false)
        end
    end

    return outcome
end

function transport_send(socket::AbstractPlainSocket, msg::RpcReqMsg)
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
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZRouter, msg::RpcReqMsg)
    socket = z.sock
    address = z.zaddress
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, nothing]), more=true)
        send(socket, data2message(msg.data), more=true)
        send(socket, MESSAGE_END, more=false)
    end

    return true
end

function transport_send(z::ZDealer, msg::RpcReqMsg, enc=false)
    #content = get_content(rb, msg.data)
    socket = z.sock
    content = data2message(msg.data)
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)

        if msg.target === nothing
            target = nothing
        else
            target = msg.target
        end

        send(socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, target]), more=true)
        send(socket, content, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(z::ZRouter, msg::ResMsg)
    socket = z.sock
    address = z.zaddress
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
        data = data2message(msg.data)
        send(socket, data, more=true)
        send(socket, MESSAGE_END, more=false)
    end

    return true
end

function transport_send(z::ZDealer, msg::ResMsg)
    socket = z.sock
    content = data2message(msg.data)
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
        send(socket, content, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::ResMsg, enc=false)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [
        TYPE_RESPONSE | msg.flags,
        id2bytes(msg.id),
        msg.status,
        message2data(content)
    ]
    transport_write(socket, pkt)
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::AdminReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_ADMIN | msg.flags, id2bytes(msg.id), msg.topic, content]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZDealer, msg::AdminReqMsg)
    socket = z.sock
    content = data2message(msg.data)
    target = nothing
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ADMIN, id2bytes(msg.id), msg.topic, target]), more=true)
        send(socket, content, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(z::ZRouter, msg::AdminReqMsg)
    socket = z.sock
    address = z.zaddress
    content = tagvalue_if_dataframe(msg.data)
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ADMIN, id2bytes(msg.id), msg.topic]), more=true)
        send(socket, encode(content), more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::AckMsg)
    pkt = [TYPE_ACK, id2bytes(msg.id)]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZRouter, msg::AckMsg)
    socket = z.sock
    address = z.zaddress
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ACK, id2bytes(msg.id)]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::Ack2Msg)
    pkt = [TYPE_ACK2, id2bytes(msg.id)]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZRouter, msg::Ack2Msg)
    socket = z.sock
    address = z.zaddress
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ACK2, id2bytes(msg.id)]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(z::ZDealer, msg::Ack2Msg)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ACK2, id2bytes(msg.id)]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(z::ZDealer, msg::AckMsg)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_ACK, id2bytes(msg.id)]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::Attestation)
    pkt = [TYPE_ATTESTATION, id2bytes(msg.id), msg.cid, msg.signature, msg.meta]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZDealer, msg::Attestation)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(
            socket,
            encode([TYPE_ATTESTATION, id2bytes(msg.id), msg.cid, msg.meta]),
            more=true
        )
        send(socket, msg.signature, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::Register)
    pkt = [TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.tenant, msg.pubkey, msg.type]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZDealer, msg::Register)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(
            socket,
            encode([TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.tenant, msg.type]),
            more=true
        )
        send(socket, msg.pubkey, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(socket::AbstractPlainSocket, msg::Unregister)
    pkt = [TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]
    transport_write(socket, pkt)
    return true
end

function transport_send(z::ZDealer, msg::Unregister)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

function transport_send(z::ZDealer, ::Close)
    socket = z.sock
    lock(zmqsocketlock) do
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_CLOSE]), more=true)
        send(socket, DATA_EMPTY, more=true)
        send(socket, MESSAGE_END, more=false)
    end
    return true
end

#=
Convert data or elements of data to a TAG value if data is/contains a DataFrame.

Return the transformed value if a DataFrame is encountered or return the
data argument otherwise.
=#
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
    HTTP.WebSockets.send(ws, payload)
end

function transport_write(ws::WS, pkt)
    payload = encode_partial(pkt)
    ws_write(ws.sock, payload)
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
    write(sock, io.data)
    flush(sock)
end

function transport_write(tcp, llmsg)
    payload = encode_partial(llmsg)
    tcp_write(tcp.sock, payload)
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
    return payload
end

transport_read(socket::WebSockets.WebSocket) = HTTP.WebSockets.receive(socket)

isconnectionerror(::Float, e) = false

function isconnectionerror(::WS, e)
    isa(e, EOFError) ||
        isa(e, Base.IOError) ||
        isa(e, WebSockets.WebSocketError)
end

function isconnectionerror(::AbstractPlainSocket, e)
    return isa(e, EOFError) || isa(e, Base.IOError) || isa(e, WrongTcpPacket)
end

function close_is_ok(::WS, e)
    HTTP.WebSockets.isok(e)
end

function close_is_ok(::AbstractPlainSocket, e)
    !isa(e, WrongTcpPacket)
end

function encode_partial(data::Vector)
    io = IOBuffer()
    encode_partial(io, data)
    return take!(io)
end

function add_payload(io, payload)
    if isa(payload, Base.GenericIOBuffer)
        pos = position(payload)
        write(io, payload)
        seek(payload, pos)
    elseif isa(payload, ZMQ.Message)
        write(io, Vector{UInt8}(payload))
    else
        encode(io, payload)
    end
end

function encode_partial(io, data::Vector)
    type = data[1] & 0x0f
    if type == TYPE_PUB
        if (data[1] & QOS1) == QOS1
            write(io, 0x84)
        else
            write(io, 0x83)
        end
        encode(io, data[1]) # type
        if (data[1] & QOS1) == QOS1
            encode(io, data[2]) # msgid
            encode(io, data[3]) # topic
            add_payload(io, data[4])
        else
            encode(io, data[2]) # topic
            add_payload(io, data[3])
        end
    elseif type == TYPE_IDENTITY
        write(io, 0x84)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # cid
        encode(io, data[4]) # meta
    elseif type == TYPE_RPC
        write(io, 0x85)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # topic
        encode(io, data[4]) # target
        add_payload(io, data[5])
    elseif type == TYPE_ADMIN
        write(io, 0x84)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # topic
        add_payload(io, data[4])
    elseif type == TYPE_RESPONSE
        write(io, 0x84)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # status
        add_payload(io, data[4])
    elseif type == TYPE_REGISTER
        write(io, 0x86)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # cid
        encode(io, data[4]) # tenant
        encode(io, data[5]) # pubkey
        encode(io, data[6]) # type
    elseif type == TYPE_UNREGISTER
        write(io, 0x83)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # cid
    elseif type == TYPE_ATTESTATION
        write(io, 0x85)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
        encode(io, data[3]) # cid
        encode(io, data[4]) # signature
        encode(io, data[5]) # meta
    elseif type == TYPE_ACK
        write(io, 0x82)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
    elseif type == TYPE_ACK2
        write(io, 0x82)
        encode(io, data[1]) # type
        encode(io, data[2]) # id
    end
end
