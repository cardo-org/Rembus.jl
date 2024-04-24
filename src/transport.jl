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

#=
    zmq_load(socket::ZMQ.Socket)

Get a Rembus message from a ZeroMQ multipart message.

The decoding is performed at the client side.
=#
function zmq_load(socket::ZMQ.Socket)

    pkt = zmq_message(socket)

    header = pkt.header
    data::Vector{UInt8} = pkt.data

    type = header[1]
    ptype = type & 0x3f
    flags = type & 0xc0
    if ptype == TYPE_PUB
        topic = header[2]
        h = UInt128(hash(topic)) + UInt128(hash(data)) << 64
        msg = PubSubMsg(topic, dataframe_if_tagvalue(decode(data)), flags, h)
    elseif ptype == TYPE_RPC
        id = bytes2id(header[2])
        topic = header[3]
        target = header[4]
        msg = RpcReqMsg(id, topic, dataframe_if_tagvalue(decode(data)), target, flags)
    elseif ptype == TYPE_RESPONSE
        id = bytes2id(header[2])
        status = header[3]
        # NOTE: for very large dataframes decode is slow, needs investigation.
        try
            val = decode(data)
            return ResMsg(id, status, dataframe_if_tagvalue(val), flags)
        catch e
            return ResMsg(id, STS_GENERIC_ERROR, "$e", flags)
        end
    end
    msg
end

#=
    connected_socket_load(packet)

Get a Rembus message decoding the CBOR packet.

The decoding is performed at the component side.
=#
function connected_socket_load(packet)
    payload = decode(packet)

    ptype = payload[1] & 0x3f
    flags = payload[1] & 0xc0
    if ptype == TYPE_PUB
        h = UInt128(hash(payload[2])) + UInt128(hash(payload[3])) << 64
        data = dataframe_if_tagvalue(payload[3])
        #                  topic     data
        return PubSubMsg(payload[2], data, flags, h)
    elseif ptype == TYPE_RPC
        if length(payload) == 5
            data = dataframe_if_tagvalue(payload[5])
        else
            data = dataframe_if_tagvalue(payload[4])
        end
        #                       id               topic     data
        return RpcReqMsg(bytes2id(payload[2]), payload[3], data)
    elseif ptype == TYPE_RESPONSE
        data = dataframe_if_tagvalue(payload[4])
        #                          id        status     data
        return ResMsg(bytes2id(payload[2]), payload[3], data, flags)
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
    ptype = type & 0x3f
    flags = type & 0xc0

    if ptype == TYPE_IDENTITY
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        @debug "<<message IDENTITY, cid: $cid)"

        if isa(cid, AbstractString)
            return IdentityMsg(bytes2id(id), cid)
        else
            error("undefined cid")
        end
    elseif ptype == TYPE_PUB
        # do not decode dataframe, just pass through the broker
        topic = decode_internal(io, Val(TYPE_3))
        @debug "<<message PUB, topic: $topic"
        return PubSubMsg(topic, io, flags)
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
        @debug "<<message ACK: id: $(payload[2])"
        return AckMsg(bytes2id(payload[2]))
    elseif ptype == TYPE_REGISTER
        id = decode_internal(io, Val(TYPE_2))
        cid = decode_internal(io)
        userid = decode_internal(io)
        pubkey = decode_internal(io)
        @debug "<<message REGISTER, cid: $cid"
        return Register(bytes2id(id), cid, userid, pubkey)
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

const MESSAGE_END = UInt8[0xde, 0xad, 0xbe, 0xef, 0xde, 0xad, 0xbe, 0xef]

mutable struct ZMQPacket
    identity::Vector{UInt8}
    header::Vector{Any}
    data::ZMQ.Message
end

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
                @error "ZMQ channel: expected empty message"
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
                @error "ZMQ channel: expected end of message"
                continue
            end

            return ZMQDealerPacket(header, data)

        catch e
            @error "wrong header $bval ($e)"
            if isempty(bval)
                expect_empty = false
            end
        end
    end
end

#=
    zmq_message(router::Router)::ZMQPacket

Receive a Multipart ZeroMQ message.

Return the packet identity, header and data values extracted from a ROUTER socket.
=#
function zmq_message(router::Router)::ZMQPacket
    expect_empty = true
    cached_identity = nothing
    while true
        if cached_identity === nothing
            identity = tobytes(router.zmqsocket)
        else
            identity = cached_identity
            cached_identity = nothing
        end

        if length(identity) != 5 || identity[1] != 0
            @error "expected identity, got: $identity"
            continue
        end

        # empty frame
        if expect_empty
            msg = ZMQ.recv(router.zmqsocket)
            if !isempty(msg)
                @error "from [$identity]: expected empty message"
                # msg may be the identity of the next message
                cached_identity = Vector{UInt8}(msg)
                continue
            end
        else
            expect_empty = true
        end

        bval = tobytes(router.zmqsocket)
        try
            header = decode(bval)
            data = recv(router.zmqsocket)

            msgend = tobytes(router.zmqsocket)
            if isempty(msgend)
                # data may be the identity of the next message
                cached_identity = data
                expect_empty = false
            elseif msgend != MESSAGE_END
                @error "from [$identity]: expected end of message"
                # msgend may be the identity of the next message
                cached_identity = msgend
                continue
            end

            return ZMQPacket(identity, header, data)

        catch e
            @error "from [$identity]: wrong header $bval ($e)"
            # header may be the identity of the next message
            cached_identity = bval
        end
    end
end

#=
    broker_parse(router::Router, pkt::ZMQPacket)

AAAA The Broker parser of ZeroMQ messages.

`pkt` is the zeromq message decoded as `[identity, header, data]`.
=#
function broker_parse(router::Router, pkt::ZMQPacket)
    id = pkt.identity
    type = pkt.header[1]

    ptype = type & 0x3f
    flags = type & 0xc0

    @debug "[zmq parse] from $id recv type $type"

    if ptype == TYPE_IDENTITY
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        if isempty(cid)
            error("empty cid")
        end
        router.mid2address[mid] = id
        return IdentityMsg(mid, cid)
    elseif ptype == TYPE_PUB
        topic = pkt.header[2]
        data = pkt.data
        return PubSubMsg(topic, data, flags)
    elseif ptype == TYPE_RPC
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        target = pkt.header[4]
        data = pkt.data
        router.mid2address[mid] = id
        return RpcReqMsg(mid, topic, data, target, flags)
    elseif ptype == TYPE_ADMIN
        mid = bytes2id(pkt.header[2])
        topic = pkt.header[3]
        data = message2data(pkt.data)
        router.mid2address[mid] = id
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
        userid = pkt.header[4]
        pubkey::Vector{UInt8} = pkt.data
        router.mid2address[mid] = id
        return Register(mid, cid, userid, pubkey)
    elseif ptype == TYPE_UNREGISTER
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        router.mid2address[mid] = id
        return Unregister(mid, cid)
    elseif ptype == TYPE_ATTESTATION
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        signature::Vector{UInt8} = pkt.data
        router.mid2address[mid] = id
        return Attestation(mid, cid, signature)
    elseif ptype == TYPE_PING
        mid = bytes2id(pkt.header[2])
        cid = pkt.header[3]
        @debug "ping from [$cid]"
        router.mid2address[mid] = id
        return PingMsg(mid, cid)
    elseif ptype == TYPE_REMOVE
        return Remove()
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

data2payload(data) = data

function data2payload(data::IOBuffer)
    mark(data)
    decode(read(data))
    reset(data)
end

# Return data to be sent via ws or tcp from ZMQ
function data2payload(data::ZMQ.Message)
    # allocate instead of consuming message
    decode(Vector{UInt8}(data))
end

function transport_file_io(msg::PubSubMsg)
    mark(msg.data)
    seek(msg.data, 2)
    payload = read(msg.data)
    len::UInt32 = length(payload) + 1
    io = IOBuffer(maxsize=4 + len)
    write(io, len)
    write(io, 0x82)
    write(io, payload)
    reset(msg.data)
    return io
end

function transport_file_io(msg::PubSubMsg{ZMQ.Message})
    payload = encode([msg.topic, data2payload(msg.data)])
    len::UInt32 = length(payload)
    io = IOBuffer(maxsize=4 + len)
    write(io, len)
    write(io, payload)
    io
end

function id2bytes(id::UInt128)
    io = IOBuffer(maxsize=16)
    write(io, id)
    io.data
end

function bytes2id(buff::Vector{UInt8})
    read(IOBuffer(buff), UInt128)
end

function bytes2zid(buff::Vector{UInt8})
    UInt128(read(IOBuffer(buff[2:end]), UInt32))
end

function transport_send(socket::ZMQ.Socket, msg::PingMsg)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_PING, id2bytes(msg.id), msg.cid]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
end

transport_send(::Twin, ws, msg::IdentityMsg) = transport_send(ws, msg)

function transport_send(ws, msg::IdentityMsg)
    pkt = [TYPE_IDENTITY, id2bytes(msg.id), msg.cid]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::IdentityMsg)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_IDENTITY, id2bytes(msg.id), msg.cid]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
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

function transport_send(twin::Twin, ws, msg::PubSubMsg)
    if twin.qos === with_ack
        msg.flags |= 0x80
        msgid = UInt128(hash(msg.topic)) + UInt128(hash(msg.data)) << 64
        twin.acktimer[msgid] = Timer((tim) -> handle_ack_timeout(
                tim, twin, msg, msgid
            ), ACK_WAIT_TIME)
    end

    pkt = [TYPE_PUB | msg.flags, msg.topic, msg.data]
    broker_transport_write(ws, pkt)

end

function transport_send(ws, msg::PubSubMsg)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_PUB | msg.flags, msg.topic, message2data(content)]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::PubSubMsg)
    content = tagvalue_if_dataframe(msg.data)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_PUB | msg.flags, msg.topic]), more=true)
    send(socket, encode(content), more=true)
    send(socket, MESSAGE_END, more=false)
end

function transport_send(twin::Twin, socket::ZMQ.Socket, msg::PubSubMsg)
    address = twin.router.twin2address[twin.id]
    data = data2message(msg.data)
    if twin.qos === with_ack
        msg.flags |= 0x80
        msgid = UInt128(hash(msg.topic)) + UInt128(hash(data)) << 64
        twin.acktimer[msgid] = Timer((tim) -> handle_ack_timeout(
                tim, twin, msg, msgid
            ), ACK_WAIT_TIME)
    end

    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_PUB | msg.flags, msg.topic]), more=true)
        send(socket, data, more=true)
        send(socket, MESSAGE_END, more=false)
    end
end

function transport_send(::Twin, ws, msg::RpcReqMsg)
    pkt = [
        TYPE_RPC | msg.flags,
        id2bytes(msg.id),
        msg.topic,
        msg.target,
        msg.data
    ]
    broker_transport_write(ws, pkt)
end

function transport_send(ws, msg::RpcReqMsg)
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
    transport_write(ws, pkt)
end

function transport_send(twin::Twin, socket::ZMQ.Socket, msg::RpcReqMsg)
    address = twin.router.twin2address[twin.id]
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, nothing]), more=true)
        send(socket, data2message(msg.data), more=true)
        send(socket, MESSAGE_END, more=false)
    end
end

function transport_send(socket::ZMQ.Socket, msg::RpcReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    send(socket, Message(), more=true)

    if msg.target === nothing
        target = nothing
    else
        target = msg.target
    end

    send(socket, encode([TYPE_RPC, id2bytes(msg.id), msg.topic, target]), more=true)
    send(socket, encode(msg.data), more=true)
    send(socket, MESSAGE_END, more=false)
end

function transport_send(twin::Twin, socket::ZMQ.Socket, msg::ResMsg, enc=false)
    address = twin.router.mid2address[msg.id]
    lock(zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        send(socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
        if enc
            data = encode(msg.data)
        else
            data = data2message(msg.data)
        end
        send(socket, data, more=true)
        send(socket, MESSAGE_END, more=false)
    end
end

function transport_send(socket::ZMQ.Socket, msg::ResMsg)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_RESPONSE, id2bytes(msg.id), msg.status]), more=true)
    send(socket, encode(msg.data), more=true)
    send(socket, MESSAGE_END, more=false)
end

function transport_send(::Twin, ws, msg::ResMsg, enc=false)
    pkt = [TYPE_RESPONSE | msg.flags, id2bytes(msg.id), msg.status, msg.data]
    broker_transport_write(ws, pkt)
end

function transport_send(ws, msg::ResMsg, ::Bool=false)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_RESPONSE | msg.flags, id2bytes(msg.id), msg.status, message2data(content)]
    transport_write(ws, pkt)
end

transport_send(::Twin, ws, msg::AdminReqMsg) = transport_send(ws, msg::AdminReqMsg)

function transport_send(ws, msg::AdminReqMsg)
    content = tagvalue_if_dataframe(msg.data)
    pkt = [TYPE_ADMIN | msg.flags, id2bytes(msg.id), msg.topic, content]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::AdminReqMsg)
    if isa(msg.data, DataFrame)
        content = tagvalue_if_dataframe(msg.data)
    else
        content = msg.data
    end
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_ADMIN, id2bytes(msg.id), msg.topic]), more=true)
    send(socket, encode(content), more=true)
    send(socket, MESSAGE_END, more=false)
end

transport_send(::Twin, ws, msg::AckMsg) = transport_send(ws, msg::AckMsg)

function transport_send(ws, msg::AckMsg)
    pkt = [TYPE_ACK, id2bytes(msg.hash)]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::AckMsg)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_ACK, id2bytes(msg.hash)]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
end

transport_send(::Twin, ws, msg::Attestation) = transport_send(ws, msg::Attestation)

function transport_send(ws, msg::Attestation)
    pkt = [TYPE_ATTESTATION, id2bytes(msg.id), msg.cid, msg.signature]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::Attestation)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_ATTESTATION, id2bytes(msg.id), msg.cid]), more=true)
    send(socket, msg.signature, more=true)
    send(socket, MESSAGE_END, more=false)
end

transport_send(::Twin, ws, msg::Register) = transport_send(ws, msg::Register)

function transport_send(ws, msg::Register)
    pkt = [TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.userid, msg.pubkey]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::Register)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_REGISTER, id2bytes(msg.id), msg.cid, msg.userid]), more=true)
    send(socket, msg.pubkey, more=true)
    send(socket, MESSAGE_END, more=false)
end

transport_send(::Twin, ws, msg::Unregister) = transport_send(ws, msg::Unregister)

function transport_send(ws, msg::Unregister)
    pkt = [TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]
    transport_write(ws, pkt)
end

function transport_send(socket::ZMQ.Socket, msg::Unregister)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_UNREGISTER, id2bytes(msg.id), msg.cid]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
end

function transport_send(socket::ZMQ.Socket, ::Close)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_CLOSE]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
end

function transport_send(socket::ZMQ.Socket, ::Remove)
    send(socket, Message(), more=true)
    send(socket, encode([TYPE_REMOVE]), more=true)
    send(socket, DATA_EMPTY, more=true)
    send(socket, MESSAGE_END, more=false)
end

function tagvalue_if_dataframe(data)
    if isa(data, Vector)
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
    try
        HTTP.WebSockets.send(ws, payload)
    catch e
        @error e
        @showerror e
        throw(RembusDisconnect())
    end
end

function broker_transport_write(ws::WebSockets.WebSocket, pkt)
    payload = encode_partial(pkt)
    ws_write(ws, payload)
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

function transport_read(sock::MbedTLS.SSLContext)
    while true
        headers = read(sock, 1)
        if isempty(headers)
            MbedTLS.ssl_session_reset(sock)
            throw(ConnectionClosed())
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
            @error "tcp channel invalid header format"
            throw(ConnectionClosed())
        end
        payload = read(sock, len)
        @rawlog("in: $payload")
        return payload
    end
end

function transport_read(sock::TCPSocket)
    while true
        headers = read(sock, 1)
        if isempty(headers)
            throw(ConnectionClosed())
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
            throw(ConnectionClosed())
        end
        payload = read(sock, len)
        @rawlog("in: $payload")
        return payload
    end
end

function transport_read(socket::WebSockets.WebSocket)
    d = HTTP.WebSockets.receive(socket)
    @rawlog("in: $d ($(typeof(d)))")
    d
end

function isconnectionerror(ws::WebSockets.WebSocket, e)
    isa(e, EOFError) || isa(e, Base.IOError) || !WebSockets.isok(ws)
end

function isconnectionerror(ws, e)
    return isa(e, EOFError) || isa(e, Base.IOError) || isa(e, ConnectionClosed)
end

Base.isopen(ws::WebSockets.WebSocket) = Base.isopen(ws.io)
