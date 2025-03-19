include("../utils.jl")

#=
    ha_pub_publisher -- ha_pub_main -- ha_pub_subscriber
                |
                |
           ha_pub_failover
=#

Rembus.info!()

broker_name = "ha_pub_main"
failover_name = "ha_pub_failover"

function mytopic(ctx, rb, msg)
    tid = Rembus.tid(rb)
    @info "[ha_pub][$(Rembus.path(rb))] recv: $msg"
    ctx[tid] = msg
end

function run()
    ctx = Dict()
    msg = "ola"
    bro = broker(ws=8000, name=broker_name)
    failover_url = "ws://:8001"
    publisher = component("ha_pub_publisher", failovers=[failover_url])
    subscriber = component("ha_pub_subscriber", failovers=[failover_url])

    failover = component(failover_name, ws=8001)

    inject(subscriber, ctx)
    subscribe(subscriber, mytopic)
    reactive(subscriber)

    inject(failover, ctx)
    subscribe(failover, mytopic)
    reactive(failover)

    #sleep(1)
    publish(publisher, "mytopic", msg)
    sleep(2)
    # Stop the main broker.
    shutdown(bro)

    # Wait for the failover to take over.
    sleep(3)

    @info "CTX: $ctx"
    @info "[$bro] subscribers: $(bro.router.topic_interests)"

    empty!(ctx)
    publish(publisher, "mytopic", msg)
    sleep(0.1)
    @info "FAILOVER CTX: $ctx"
    @test length(ctx) === 2

    #    # Restart the main broker.
    #    bro = broker(ws=8000, name=broker_name)
    #
    #    # Wait for the main broker to take over.
    #    sleep(3)

    shutdown()
end

@info "[ha_pub] start"
try
    run()
catch e
    @test false
    @error "[ha_pub] error: $e"
finally
    shutdown()
end
@info "[ha_pub] stop"
