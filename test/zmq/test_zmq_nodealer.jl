include("../utils.jl")
using ZMQ

function run()
    rb = Rembus.RBConnection("zmq://:8002/myzmq")

    rb.context = ZMQ.Context()
    rb.socket = ZMQ.Socket(rb.context, REQ)
    url = Rembus.brokerurl(rb.client)
    @info "url:$url"
    ZMQ.connect(rb.socket, url)

    # send a message and expect a timeout
    @test_throws RembusTimeout Rembus.authenticate(rb)

    sleep(1)
end

execute(run, "test_zmq_nodealer", args=Dict("zmq" => 8002))
