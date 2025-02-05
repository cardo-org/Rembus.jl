include("../utils.jl")

myservice() = 1

component_topic() = nothing


function component_service()
    return 1
end

function start_servers()
    s1 = server(ws=7000)
    expose(s1, myservice)

    s2 = server(ws=7001)
    expose(s2, myservice)
end

function run()
    rb = connect(["ws://:7000", "ws://:7001"])

    subscribe(rb, component_topic, wait=false)
    expose(rb, component_service)

    fut = rpc(rb, "myservice", timeout=3, raise=false, wait=false)
    response = fetch_response(fut)
    @test response == 1

    unexpose(rb, component_service)
    close(rb)

    # no connections are available
    rb = connect(["ws://:6000", "ws://:6001"])

    @test_throws RembusError rpc(rb, "myservice")

    response = rpc(rb, "myservice", raise=false)
    @test isa(response, RembusError)

    fut = rpc(rb, "myservice", wait=false)
    @test_throws RembusError fetch_response(fut)
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
