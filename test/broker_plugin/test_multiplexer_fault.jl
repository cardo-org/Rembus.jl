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

function run(exposer_url)
    # main broker
    caronte(
        wait=false,
        args=Dict("name" => "main_broker", "reset" => true)
    )
    yield()

    caronte(
        wait=false,
        plugin=Broker,
        args=Dict("name" => "edge_broker", "ws" => 9000))
    yield()

    #    cli_url = "ws://:8000/client"
    broker_url = "ws://:8000/combo"

    Rembus.egress(broker_url, "edge_broker")
    sleep(1)
    @info "shutting down main broker"

    # shutting down the main_broker triggers an error on the edge_broker
    # and the supervisor
    main_broker = from("main_broker")
    shutdown(main_broker)
    sleep(1)

    p = from(broker_url)
    @test p.status === Visor.failed

    # reconnect
    caronte(
        wait=false,
        args=Dict("name" => "main_broker", "reset" => true)
    )
    sleep(5)
    p = from(broker_url)
    @test p.status === Visor.running
end

ENV["REMBUS_CONNECT_TIMEOUT"] = 20
run("ws://:9000/server")
shutdown()
delete!(ENV, "REMBUS_CONNECT_TIMEOUT")
