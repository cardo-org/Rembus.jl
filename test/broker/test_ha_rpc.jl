include("../utils.jl")

#=
    ha_c1 -- ha_main -- ha_c2
                |
                |
           ha_failover
=#

Rembus.info!()

broker_name = "ha_main"
failover_name = "ha_failover"

myservice(x, y) = x + y

function run()
    x = 1
    y = 2
    bro = broker(ws=8000, name=broker_name)
    failover_url = "ws://:8001"
    c1 = component("ha_c1", failovers=[failover_url])
    c2 = component("ha_c2", failovers=[failover_url])

    failover = component(failover_name, ws=8001)
    expose(c2, myservice)

    @test rpc(c1, "rid") === broker_name
    @test rpc(c1, "myservice", x, y) == x + y

    # Stop the main broker.
    shutdown(bro)

    # Wait for the failover to take over.
    sleep(3)

    @test rpc(c1, "rid") === failover_name

    @test rpc(c1, "myservice", x, y) == x + y
    @info "[$bro] exposers: $(bro.router.topic_impls)"
    @info "[$failover] exposers: $(failover.router.topic_impls)"

    # Restart the main broker.
    bro = broker(ws=8000, name=broker_name)

    # Wait for the main broker to take over.
    sleep(3)

    @test rpc(c1, "rid") === broker_name
    @test rpc(c1, "myservice", x, y) == x + y

    shutdown()
end

@info "[ha_rpc] start"
try
    run()
catch e
    @test false
    @error "[ha_rpc] error: $e"
finally
    shutdown()
end
@info "[ha_rpc] stop"
