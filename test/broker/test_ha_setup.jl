include("../utils.jl")

#=
    ha_setup_c1 -- ha_setup_main
                       |
                       |
                ha_setup_failover
=#

Rembus.info!()

broker_name = "ha_setup_main"
failover_name = "ha_setup_failover"

myservice(x, y) = x + y

mutable struct HAProxy <: Rembus.AbstractRouter
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function HAProxy()
        return new(nothing, nothing)
    end
end

function proxy_task(self, router)
    for msg in self.inbox
        @debug "[ha_setup_proxy] recv: $msg"
        !isshutdown(msg) || break

        if isa(msg, Rembus.AdminReqMsg) &&
           haskey(msg.data, Rembus.COMMAND) &&
           msg.data[Rembus.COMMAND] === Rembus.SETUP_CMD
            @info "[ha_setup_proxy][$(msg.twin)] simulate an error: $msg"
            put!(msg.twin.process.inbox, Rembus.ResMsg(msg, Rembus.STS_GENERIC_ERROR))
        else
            # route to downstream broker
            put!(router.downstream.process.inbox, msg)
        end
    end
end

function start_proxy(supervisor_name, downstream_router)
    proxy = HAProxy()
    Rembus.upstream!(downstream_router, proxy)
    sv = from(supervisor_name)
    proxy.process = process("proxy", proxy_task, args=(proxy,))
    startup(sv, proxy.process)
end

function start_broker(name)
    router = Rembus.get_router(name=name, ws=8000)
    start_proxy(name, router)
    return Rembus.bind(router)
end

function run()
    x = 1
    y = 2
    bro = start_broker(broker_name)

    failover_url = "ws://:8001"
    c1 = component("ha_setup_c1", failovers=[failover_url])

    failover = component(failover_name, ws=8001)
    expose(failover, myservice)

    @test rpc(c1, "rid") === broker_name
    @test rpc(c1, "myservice", x, y) == x + y

    # Stop the main broker.
    shutdown(bro)

    # Wait for the failover to take over.
    sleep(3)

    @test rpc(c1, "rid") === failover_name

    @test rpc(c1, "myservice", x, y) == x + y
    @info "[$bro] exposers: $(Rembus.last_downstream(bro.router).topic_impls)"
    @info "[$failover] exposers: $(failover.router.topic_impls)"

    # Restart the main broker.
    bro = start_broker(broker_name)

    # Wait for the main broker to try the take over ...
    sleep(3)

    # but the simulated error prevents the switch to main broker.
    @test rpc(c1, "rid") === failover_name
    @test rpc(c1, "myservice", x, y) == x + y

    shutdown()
end

@info "[ha_setup] start"
try
    run()
catch e
    @test false
    @error "[ha_setup] error: $e"
finally
    shutdown()
end
@info "[ha_setup] stop"
