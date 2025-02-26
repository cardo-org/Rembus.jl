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
    rb = Rembus.connect("zmq://:8002/dealer_zmq_cut")

    res = rpc(rb, "version")
    @info "response: $res"
    sleep(3)

    broker = from("dealer_zmq.broker")
    router = broker.args[1]
    socket = router.zmqsocket

    @info "[TEST][$router] $(pointer_from_objref(router)) address2twin: $(router.address2twin)"

    identity = first(keys(router.address2twin))
    @info "identity: $(identity)"

    # just logs an error
    wrong_message_type(identity, socket)
    no_empty_message(identity, socket)
    ok_message(identity, socket)
    no_msgend(identity, socket)
    msgempty_insteadof_msgend(identity, socket)
    wrong_header_1(identity, socket)
    wrong_header_2(identity, socket)

    sleep(3)
    res = rpc(rb, "version")
    @test isa(res, String)
    sleep(0.2)

    sleep(1)
    close(rb)
end

execute(run, "dealer_zmq")
