include("../utils.jl")

myservice() = 1

mytopic() = nothing

function start_servers()
    s1 = server(ws=7000)
    expose(s1, myservice)

    s2 = server(ws=7001)
    expose(s2, myservice)
end

function run()
    rb = connect(["ws://:7000", "ws://:7001"])

    subscribe(rb, mytopic, wait=false)

    fut = rpc(rb, "myservice", timeout=3, exceptionerror=false, wait=false)
    response = fetch_response(fut)
    @info fut
    @test response == 1

    close(rb)
end


@info "[test_rpc_future] start"
try
    start_servers()
    run()
catch e
    @error "[test_rpc_future] error: $e"
    @test false
finally
    shutdown()
end

@info "[test_rpc_future] stop"
