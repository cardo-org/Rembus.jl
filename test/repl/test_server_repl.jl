include("../utils.jl")

using DataFrames
using HTTP

Base.isinteractive() = true

#function rpc_service(x)
#    return x
#end

#rpc_service(ctx, rb, x) = return x

#function rpc_service(x, y)
#    return x + y
#end

function start_server()
    rb = broker(ws=8000, name="server_repl")

    rpc_service(x) = x
    expose(rb, rpc_service)

    rpc_service(ctx, rb, x) = x
    rpc_service(x, y) = x + y

    return rb
end


function run()
    try
        srv = start_server()
        sleep(2)
        rb = connect()
        result = rpc(rb, "rpc_service", [1, 2])
        @test result == 3
        result = rpc(rb, "rpc_service", 1)
        @test result == 1

        inject(srv)
        result = rpc(rb, "rpc_service", 1)
        @test result == 1

        result = rpc(rb, "rpc_service", [1])
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
