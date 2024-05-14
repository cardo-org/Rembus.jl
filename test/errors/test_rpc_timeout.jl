include("../utils.jl")

function myservice()
    sleep(5)
    return 1
end

function yourservice()
    error("your service failed")
end

function run()
    rb = connect()
    server = connect()
    try
        expose(server, myservice)
        expose(server, yourservice)
        sleep(1)
        rpc(rb, "myservice")
    catch e
        @info "[test_rpc_timeout] expected error: $e"
        @test isa(e, RembusTimeout)
    end

    try
        rpc(rb, "yourservice")
    catch e
        @info "[test_rpc_timeout] expected error: $e"
        @test isa(e, Rembus.RpcMethodException)
    end
    sleep(1)
    close(rb)
    close(server)


end

rembus_timeout = Rembus.request_timeout()
ENV["REMBUS_TIMEOUT"] = 4
execute(run, "test_rpc_timeout")
ENV["REMBUS_TIMEOUT"] = rembus_timeout
