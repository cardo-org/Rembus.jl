include("../utils.jl")
using ZMQ

function wrong_message_type(socket)
    identity = zeros(UInt8, 5)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    # send a wrong message id 0x17
    send(socket, encode([0x17, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function wrong_identity(socket)
    wrong_identity = zeros(UInt8, 4)

    send(socket, wrong_identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_empty_message(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, ok_identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "aaa", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function ok_message(socket)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_header_is_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, ok_identity, more=true)  # should be header
    send(socket, Message(), more=true)    # should be data
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true) # should be msgend
    send(socket, "bbb", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_header_is_not_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, ok_identity, more=true)  # should be header
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_data_is_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, "something", more=true)  # should be header
    send(socket, ok_identity, more=true)  # should be data
    send(socket, Message(), more=true)    # should be msgend
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "ccc", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_data_is_identity_but_no_empty_msg(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, "something", more=true)  # should be header
    send(socket, ok_identity, more=true)  # should be data
    send(socket, "aaaa", more=true)    # should be msgend
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "ccc", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_data_is_not_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, "aabb", more=true)  # should be header
    send(socket, "ccdd", more=true)  # should be data
    send(socket, Message(), more=true)    # should be msgend
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "ddd", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend_msgend_is_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, "something", more=true)  # should be header
    send(socket, ok_identity, more=true)  # should be data
    send(socket, Message(), more=true)    # should be msgend
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "ccc", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function msgend_is_identity(socket)
    ok_identity = zeros(UInt8, 5)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "ccc", more=true)
    send(socket, ok_identity, more=true)
    send(socket, Message(), more=true)    # should be msgend
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "bb", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)

end

function empty_cid(socket)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_IDENTITY, Rembus.id2bytes(UInt128(1)), ""]), more=true)
    send(socket, Rembus.DATA_EMPTY, more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function run()
    context = ZMQ.Context()
    socket = ZMQ.Socket(context, DEALER)
    ZMQ.connect(socket, "tcp://127.0.0.1:8002")
    wrong_message_type(socket)
    wrong_identity(socket)
    no_empty_message(socket)
    ok_message(socket)
    no_msgend_header_is_identity(socket)
    no_msgend_header_is_not_identity(socket)
    ok_message(socket)

    no_msgend_data_is_identity(socket)
    no_msgend_data_is_identity_but_no_empty_msg(socket)

    no_msgend_data_is_not_identity(socket)
    msgend_is_identity(socket)
    empty_cid(socket)
end

execute(run, "test_client_zmq_message")
