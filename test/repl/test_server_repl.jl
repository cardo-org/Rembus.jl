include("../utils.jl")

using DataFrames
using HTTP

Base.isinteractive() = true

function start_server()
    rb = broker(ws=8000, name="server_repl")

    #rpc_service(x) = x
    rpc_service(x; ctx=nothing, node=nothing) = x
    expose(rb, rpc_service)

    rpc_service(x, y; ctx=nothing, node=nothing) = x + y
    #rpc_service(x, y) = x + y

    return rb
end


function run()
    try
        srv = start_server()
        sleep(2)
        rb = connect()
        result = rpc(rb, "rpc_service", 1, 2)
        @test result == 3
        result = rpc(rb, "rpc_service", 1)
        @test result == 1

        inject(srv)
        result = rpc(rb, "rpc_service", 1)
        @test result == 1

        shutdown(rb)
    catch e
        @error "[test_server_repl] error: $e"
        @test false
    finally
        shutdown()
        sleep(2)
    end
end

run()
