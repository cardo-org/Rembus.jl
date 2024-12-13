include("../utils.jl")

using DataFrames
using HTTP

function rpc_service(x, y)
    return x + y
end

smoke_message = "ola"
received = false

function mytopic(name)
    global received
    @info "[test_server_zmq_close] recv mytopic: $name"
    @atest name == smoke_message "expected name == $smoke_message"
    received = true
end


function start_server()
    emb = server(zmq=8002, log="info")
    expose(emb, rpc_service)
    @info "$emb setup completed"

    return emb
end


function run()
    try
        srv = start_server()
        is_up = Rembus.islistening(procs=["server.serve_zmq"])

        @component "zmq://:8002/rpc_client"
        @subscribe mytopic

        result = @rpc rpc_service(1, 2)
        @test result == 3

        publish(srv, "mytopic", "ola", qos=QOS2)
        sleep(1)

        # close the connection server-side
        conn = srv.connections[1]
        @info "closing socket $(conn.type)"
        close(conn)
        sleep(0.1)
    catch e
        @error "[test_server_zmq_close] error: $e"
        @test false
    finally
        shutdown()
    end

end

@info "[test_server_zmq_close] start"
run()
@info "[test_server_zmq_close] stop"
