include("../utils.jl")

using DataFrames

function df_service(session)
    DataFrame(x=1:10)
end

function rpc_service(session, x, y)
    return x + y
end

function rpc_fault(session, x::Integer, y::Integer)
    return x // y
end

smoke_message = "ola"
received = false

function signal(session, name)
    global received
    @atest name == smoke_message "expected name == $smoke_message"
    received = true
end


function start_server()
    emb = embedded()
    provide(emb, df_service)
    provide(emb, rpc_service)
    provide(emb, rpc_fault)
    provide(emb, signal)
    serve(emb, wait=false, exit_when_done=false)
end


function run()
    try
        start_server()
        sleep(2)
        result = @rpc rpc_service(1, 2)
        @test result == 3

        df = @rpc df_service()
        @test isa(df, DataFrame)

        try
            @rpc rpc_service(1)
            @test false
        catch e
            @info "[test_embedded] rpc_service error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        try
            result = @rpc rpc_fault(0, 0)
            @test false
        catch e
            @info "[test_embedded] rpc_fault error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        @publish signal(smoke_message)
        @terminate

        rb = connect()
        publish(rb, "signal", smoke_message)

        # if an error on the embedded side occurred the connection is closed
        @test Rembus.isconnected(rb)

        close(rb)
    catch e
        @error "[test_embedded] error: $e"
        @test false
    finally
        shutdown()
    end

end

run()
@test received
testsummary()
