include("../utils.jl")
using ZMQ

# tests: 1

function close_zmq_router_socket()
    broker = from("caronte.broker")
    router = broker.args[1]
    socket = router.zmqsocket
    close(socket)

end

function run()
    # invalid ssl configuration prevent ws_serve process startup
    caronte(wait=false, zmq=8002)
    sleep(1)
    zeromq_task = from("caronte.serve_zeromq").task
    close_zmq_router_socket()
    sleep(3)
    @test from("caronte.serve_zeromq").task !== zeromq_task
    shutdown()
end

@info "[test_serve_zmq_error] start"
run()
@info "[test_serve_zmq_error] stop"
