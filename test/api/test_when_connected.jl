include("../utils.jl")

function component_test()
    whenconnected(component()) do rb
        ver = rpc(rb, "version")
        @info "[component_test] version: $ver"
        @test ver == Rembus.VERSION
    end
end

function server_test()
    srv = server(ws=3000)
    bro = broker(wait=false)
    try
        add_node(bro, "ws://127.0.0.1:3000/myserver")

        whenconnected(srv) do rb
            ver = rpc(rb, "version")
            @info "[server_test] version: $ver"
            @test ver == Rembus.VERSION
        end
    catch e
        @error "[test_when_connected] server_test error: $e"
        @test false
    finally
        remove_node(bro, "ws://127.0.0.1:3000/myserver")
    end
end

try
    execute(component_test, "test_when_connected")
    server_test()
catch e
    @error "[test_when_connected] error: $e"
    @test false
finally
    shutdown()
end
