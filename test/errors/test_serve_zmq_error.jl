include("../utils.jl")
using ZMQ

function close_zmq_router_socket()
    broker = from("caronte.broker")
    router = broker.args[1]
    socket = router.zmqsocket
    close(socket)

end

function run()
    # invalid ssl configuration prevent ws_serve process startup
    caronte(wait=false, args=Dict("zmq" => 8002))
    sleep(1)
    close_zmq_router_socket()
    sleep(1)
    @test from("caronte.serve_zeromq") === nothing
end

@info "[test_serve_zmq_error] start"
run()
@info "[test_serve_zmq_error] stop"
