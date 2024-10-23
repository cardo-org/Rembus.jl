include("../utils.jl")

function rpc_service(x, y)
    return x + y
end

late_topic() = return 100;

global emb

function start_server()
    global emb
    try
        emb = server(log="debug")
        expose(emb, rpc_service)
    catch e
        @error "[test_server_late_expose] error: $e"
    end
end

function run()
    try
        @async start_server()
        sleep(2)
        result = @rpc rpc_service(1, 2)
        @test result == 3

        # Expose a method after the node connects
        expose(emb, late_topic)

        result = @rpc late_topic()
        @test result == 100
    catch e
        @error "[test_embedded] error: $e"
        @test false
    finally
        shutdown()
        sleep(2)
    end

end

run()
