include("../utils.jl")

using Sockets

node = "authenticated_node"

function run()
    authenticated!()
    rb = Rembus.connect(node)

    # send a command
    response = rpc(rb, "version")
    @test isa(response, String)
    close(rb)

    rb = Rembus.connect("zmq://localhost:8002/$node")
    sleep(2)
    # send a command
    response = rpc(rb, "version")
    @test isa(response, String)
    close(rb)

    # anonymous components not allowed
    @test_throws ErrorException Rembus.connect()

    # connect without sending any packet
    sock = Sockets.connect("127.0.0.1", 8001)
    close(sock)
end

try
    execute(run, "test_connect_authenticated", mode="authenticated")
    @test true
catch e
    @error "[test_connect_authenticated]: $e"
    @test false
finally
end
