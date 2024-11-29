include("../utils.jl")

function foo(x)
    @info "[foo] arg:$x"
    return x + 1
end

function subscriber(x)
    @info "[subscriber] arg: $x"
end

function run(exposer_url, secure=false)
    # main broker
    broker(
        wait=false,
        name="main_broker", reset=true, secure=secure
    )
    yield()

    edge_broker = broker(
        wait=false,
        name="edge_broker", ws=9000, secure=secure)
    yield()
    sleep(1)

    if secure
        cli_url = "wss://:8000/client"
        broker_url = "wss://:8000/combo"
    else
        cli_url = "ws://:8000/client"
        broker_url = "ws://:8000/combo"
    end

    # to be invoked in the edge_broker application
    # it connect from edge_broker to main_broker.
    connect(edge_broker, broker_url)

    @component exposer_url
    @expose foo
    @subscribe subscriber
    @reactive

    cli = connect(cli_url)
    res = rpc(cli, "foo", 1)
    @test res == 2

    publish(cli, "subscriber", 2.0)

    @shutdown
    close(cli)
end

ENV["REMBUS_CONNECT_TIMEOUT"] = 10

@info "[test_multipler] start"
try
    run("ws://:9000/server")
finally
    shutdown()
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_multiplexer"
else
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        run("wss://:9000/server", true)
        shutdown()
        # unsetting HTTP_CA_BUNDLE implies that ws_connect throws an error
        delete!(ENV, "HTTP_CA_BUNDLE")
        run("wss://:9000/server", true)

    catch e
        @error "[test_multiplexer]: $e"
    finally
        shutdown()
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end

delete!(ENV, "REMBUS_CONNECT_TIMEOUT")
@info "[test_multipler] stop"
