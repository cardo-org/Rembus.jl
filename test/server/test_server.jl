include("../utils.jl")

using DataFrames

function df_service(ctx, session)
    DataFrame(x=1:10)
end

function rpc_service(ctx, session, x, y)
    return x + y
end

function rpc_fault(ctx, session, x::Integer, y::Integer)
    return x // y
end

smoke_message = "ola"
received = false

function signal(ctx, session, name)
    global received
    @atest name == smoke_message "expected name == $smoke_message"
    received = true
end


function start_server()
    emb = server()
    provide(emb, df_service)
    provide(emb, rpc_service)
    provide(emb, rpc_fault)
    provide(emb, signal)
    serve(emb, wait=false, args=Dict("debug" => true))
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
            @info "[test_embedded] expected rpc_service error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        try
            result = @rpc rpc_fault(0, 0)
            @test false
        catch e
            @info "[test_embedded] expected rpc_fault error: $e"
            @test isa(e, Rembus.RpcMethodException)
        end

        try
            result = @rpc rpc_unknow()
        catch e
            @info "[test_embedded] expected rpc_unknow error: $e"
            @test isa(e, Rembus.RpcMethodNotFound)
        end

        @publish signal(smoke_message)
        @terminate

        rb = connect("mycomponent")

        publish(rb, "signal", smoke_message)

        # if an error on the server side occurred the connection is closed
        @test isconnected(rb)

        close(rb)
        @info "second round"
        sleep(3)
        # reconnect
        rb = connect("mycomponent")
        @test isconnected(rb)
        close(rb)

        rb = connect()
        publish(rb, "df_service", nothing)
        sleep(0.5)
        close(rb)
    catch e
        @error "[test_embedded] error: $e"
        @test false
    finally
        shutdown()
        sleep(2)
    end

end

run()
@test received
testsummary()
