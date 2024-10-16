include("../utils.jl")

node = "authenticated_node"

function run()
    authenticated!()
    rb = connect(node)

    # send a command
    response = rpc(rb, "version")
    @test isa(response, String)
    close(rb)

    rb = connect("zmq://localhost:8002/$node")
    sleep(2)
    # send a command
    response = rpc(rb, "version")
    @test isa(response, String)
    close(rb)
end

try
    execute(run, "test_connect_authenticated", mode="authenticated")
    @test true
catch e
    @error "[test_connect_authenticated]: $e"
    @test false
finally
end
