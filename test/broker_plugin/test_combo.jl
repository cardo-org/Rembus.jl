using Rembus
using Test

struct Ctx
    broker::Rembus.Twin
end

module Broker

using Rembus

function expose_handler(ctx, router, component, message)
    Rembus.transport_send(component, ctx.broker.socket, message)
end

function subscribe_handler(ctx, router, component, message)
    Rembus.transport_send(component, ctx.broker.socket, message)
end

function reactive_handler(ctx, router, component, message)
    Rembus.transport_send(component, ctx.broker.socket, message)
end


end # module Broker

function foo(x)
    @info "[foo] arg:$x"
    return x + 1
end

function subscriber(x)
    @info "[subscriber] arg: $x"
end

function run(exposer_url, secure=false)
    # main broker
    caronte(wait=false, args=Dict("reset" => true, "secure" => secure))
    yield()

    caronte(
        wait=false,
        plugin=Broker,
        args=Dict("sv_name" => "edge_broker", "ws" => 9000, "secure" => secure))
    yield()

    proc = from("edge_broker.broker")
    router = proc.args[1]

    if secure
        cli_url = "wss://:8000/client"
        broker_url = "wss://:8000/combo"
    else
        cli_url = "ws://:8000/client"
        broker_url = "ws://:8000/combo"
    end

    twin = Rembus.broker_twin(router, broker_url)
    router.context = Ctx(twin)

    @component exposer_url
    @expose foo
    @subscribe subscriber
    @reactive

    cli = connect(cli_url)
    res = rpc(cli, "foo", 1)
    @test res == 2

    publish(cli, "subscriber", 2.0)
    sleep(2)

    @terminate
    close(cli)

    return twin
end

ENV["REMBUS_CONNECT_TIMEOUT"] = 20

run("ws://:9000/server")
shutdown()

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_combo"
else
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, Rembus.REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        run("wss://:9000/server", true)
        shutdown()

        # unsetting HTTP_CA_BUNDLE implies that ws_connect throws an error
        delete!(ENV, "HTTP_CA_BUNDLE")
        run("wss://:9000/server", true)

    catch e
        @error "[test_combo]: $e"
    finally
        shutdown()
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end

delete!(ENV, "REMBUS_CONNECT_TIMEOUT")
