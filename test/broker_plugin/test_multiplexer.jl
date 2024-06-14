using Rembus
using Test

module Broker

using Rembus

function expose_handler(sock, router, component, message)
    Rembus.transport_send(component, sock, message)
end

function subscribe_handler(sock, router, component, message)
    Rembus.transport_send(component, sock, message)
end

function reactive_handler(sock, router, component, message)
    Rembus.transport_send(component, sock, message)
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
    caronte(
        wait=false,
        args=Dict("broker" => "main_broker", "reset" => true, "secure" => secure)
    )
    yield()

    caronte(
        wait=false,
        plugin=Broker,
        args=Dict("broker" => "edge_broker", "ws" => 9000, "secure" => secure))
    yield()

    if secure
        cli_url = "wss://:8000/client"
        broker_url = "wss://:8000/combo"
    else
        cli_url = "ws://:8000/client"
        broker_url = "ws://:8000/combo"
    end

    Rembus.egress(broker_url, "edge_broker")

    @component exposer_url
    @expose foo
    @subscribe subscriber
    @reactive

    cli = connect(cli_url)
    res = rpc(cli, "foo", 1)
    @test res == 2

    publish(cli, "subscriber", 2.0)

    @terminate
    close(cli)
end

ENV["REMBUS_CONNECT_TIMEOUT"] = 10

run("ws://:9000/server")
shutdown()
Visor.dump()

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_multiplexer"
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
        Visor.dump()
        # unsetting HTTP_CA_BUNDLE implies that ws_connect throws an error
        delete!(ENV, "HTTP_CA_BUNDLE")
        run("wss://:9000/server", true)

    catch e
        @error "[test_multiplexer]: $e"
        showerror(stdout, e, stacktrace())
    finally
        shutdown()
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end

delete!(ENV, "REMBUS_CONNECT_TIMEOUT")
