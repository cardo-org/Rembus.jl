include("../utils.jl")

using DataFrames
using HTTP

function df_service()
    DataFrame(x=1:10)
end

function rpc_service(x, y)
    return x + y
end

function rpc_fault(x::Integer, y::Integer)
    return x // y
end

smoke_message = "ola"
received = false

function signal(name)
    global received
    @atest name == smoke_message "expected name == $smoke_message"
    received = true
end


function start_server()
    emb = server(zmq=8002, log="info")
    expose(emb, df_service)
    expose(emb, rpc_service)
    expose(emb, rpc_fault)
    expose(emb, signal)
    @info "$emb setup completed"

    return emb
end


function run()
    try
        # set large timeout because coverage.jl is slow
        @rpc_timeout 30

        srv = start_server()
        is_up = Rembus.islistening(procs=["server.serve_zmq"])

        @component "zmq://:8002/rpc_client"
        result = @rpc rpc_service(1, 2)
        @test result == 3
        df = @rpc df_service()
        @test isa(df, DataFrame)

        try
            @rpc rpc_service(1)
            @test false
        catch e
            @info "[test_server_zmq] expected rpc_service error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        try
            result = @rpc rpc_fault(0, 0)
            @test false
        catch e
            @info "[test_server_zmq] expected rpc_fault error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        try
            result = @rpc rpc_unknow()
        catch e
            @info "[test_server_zmq] expected rpc_unknow error: $e"
            @test isa(e, Rembus.RpcMethodNotFound)
        end

        @publish signal(smoke_message)
        sleep(0.1)
        @shutdown

        rb = connect("zmq://:8002/mycomponent")

        publish(rb, "signal", smoke_message)

        # if an error on the server side occurred the connection is closed
        @test isconnected(rb)

        close(rb)
        @info "second round"
        sleep(3)
        # reconnect
        rb = connect("zmq://:8002/mycomponent")
        @test isconnected(rb)
        close(rb)

        rb = connect("zmq://:8002")
        publish(rb, "df_service")
        sleep(0.5)

        # an invalid packet logs an error on server
        HTTP.WebSockets.send(rb.socket, [0x01])
        sleep(0.1)
        @test isopen(rb.socket)

        # send an Admin command to the server
        # return successfully but on server side the AdminCommand is ignored
        Rembus.rpcreq(
            rb,
            Rembus.AdminReqMsg(
                Rembus.BROKER_CONFIG, Dict(Rembus.COMMAND => Rembus.SHUTDOWN_CMD)
            )
        )

        # close the connection server-side
        conn = srv.connections[1]
        @info "closing socket $(conn.type)"
        close(conn)
        sleep(1)
    catch e
        @error "[test_server_zmq] error: $e"
        @test false
    finally
        shutdown()
        sleep(2)
    end

end

@info "[test_server_zmq] start"
run()
@test received
testsummary()
@info "[test_server_zmq] stop"
