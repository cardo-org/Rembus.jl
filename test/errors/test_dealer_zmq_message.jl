include("../utils.jl")
using ZMQ

# tests: 1

function wrong_message_type(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    # send a wrong message id 0x17
    send(socket, encode([0x17, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_empty_message(identity, socket)
    send(socket, identity, more=true)
    send(socket, encode([0x17, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function ok_message(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function no_msgend(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, "aaa", more=false)
end

function msgempty_insteadof_msgend(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, encode([Rembus.TYPE_PUB, "topic"]), more=true)
    send(socket, "data", more=false)
    ok_message(identity, socket)

end

function wrong_header_1(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    send(socket, "zzz", more=true) # not a valid header
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function wrong_header_2(identity, socket)
    send(socket, identity, more=true)
    send(socket, Message(), more=false)
    ok_message(identity, socket)
end

function run()
    rb = Rembus.connect("zmq://:8002/cut")
    sleep(0.2)

    broker = from("$BROKER_NAME.broker")
    router = broker.args[1]
    socket = router.zmqsocket
    identity = router.twin2address["cut"]

    # just logs an error
    wrong_message_type(identity, socket)
    no_empty_message(identity, socket)
    #ok_message(identity, socket)
    no_msgend(identity, socket)
    msgempty_insteadof_msgend(identity, socket)
    wrong_header_1(identity, socket)
    wrong_header_2(identity, socket)

    res = rpc(rb, "version")
    @test isa(res, String)
    sleep(0.2)

    close(rb)
end

execute(run, "test_dealer_zmq_message")
