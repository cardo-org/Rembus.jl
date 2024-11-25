include("../utils.jl")
using ZMQ

function wrong_message_type(address, socket)
    lock(Rembus.zmqsocketlock) do
        send(socket, address, more=true)
        send(socket, Message(), more=true)
        # send a wrong message id 0x17
        send(socket, encode([0x17, "topic"]), more=true)
        send(socket, "data", more=true)
        send(socket, Rembus.MESSAGE_END, more=false)
    end
end

function run()
    srv = server(zmq=9001)

    bro = broker(wait=false, zmq=8002)
    add_node(bro, "zmq://127.0.0.1:9001/server")
    sleep(0.5)
    conns = srv.connections
    wrong_message_type(conns[1].zaddress, conns[1].socket)
    @test length(bro.servers) == 1
end

@info "[test_zmq_invalid_message] start"
try
    run()
finally
    shutdown()
end
@info "[test_zmq_invalid_message] stop"
