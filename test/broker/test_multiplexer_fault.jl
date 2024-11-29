using Rembus
using Test

function foo(x)
    @info "[foo] arg:$x"
    return x + 1
end

function subscriber(x)
    @info "[subscriber] arg: $x"
end

function run(exposer_url)
    # main broker
    broker(
        wait=false, name="main_broker", reset=true
    )
    yield()

    edge_broker = broker(
        wait=false,
        name="edge_broker", ws=9000)
    yield()

    broker_url = "ws://:8000/combo"

    connect(edge_broker, broker_url)
    sleep(1)

    connector = from_name(Rembus.cid(Rembus.RbURL(broker_url)))

    main_broker = from("main_broker")
    shutdown(main_broker)
    sleep(1)
    @test connector.status === Visor.failed

    # reconnect
    broker(
        wait=false,
        name="main_broker", reset=true
    )
    sleep(5)
    p = from_name(Rembus.cid(Rembus.RbURL(broker_url)))
    @test p.status === Visor.running
end

ENV["REMBUS_CONNECT_TIMEOUT"] = 20
try
    run("ws://:9000/server")
finally
    shutdown()
end
delete!(ENV, "REMBUS_CONNECT_TIMEOUT")
