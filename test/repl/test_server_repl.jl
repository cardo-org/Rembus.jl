include("../utils.jl")

using DataFrames
using HTTP

Base.isinteractive() = true

function rpc_service(x)
    return x
end

function rpc_service(x, y)
    return x + y
end

function start_server()
    rb = server(log="debug")
    expose(rb, rpc_service)
end


function run()
    try
        @async start_server()
        sleep(2)
        rb = connect()
        result = rpc(rb, "rpc_service", [1, 2])
        @test result == 3
        result = rpc(rb, "rpc_service", 1)
        @test result == 1
        close(rb)
    catch e
        @error "[test_server_repl] error: $e"
        @test false
    finally
        shutdown()
        sleep(2)
    end
end

run()
