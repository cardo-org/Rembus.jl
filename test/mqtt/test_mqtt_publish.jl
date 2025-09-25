using Mosquitto
using Rembus
using Test

function mytopic(ctx, rb, msg)
    @info "[mytopic] got message: $msg"
    ctx["msg"] = msg
    ctx["count"] += 1
end

function run()
    #Rembus.debug!()
    host = get(ENV, "MQTT_HOST", "localhost")
    port = parse(Int, get(ENV, "MQTT_PORT", "1883"))

    ctx = Dict{String,Any}("count" => 0)

    broker = component("mqtt://$host:$port/mosca", ws=8000)

    rb = component("mqtt_subscriber")
    inject(rb, ctx)
    Rembus.subscribe(rb, mytopic)
    reactive(rb)

    sleep(1) # wait for connection establishement
    @test isopen(broker)

    Rembus.publish(broker, "mytopic", "hello mosca")
    sleep(4) # wait for message delivery
    @info "ctx: $ctx"
    @test ctx["count"] == 1
    @test ctx["msg"] == "hello mosca"

    pub = component("mqtt_publisher")
    Rembus.publish(pub, "mytopic", "hello from publisher")
    sleep(4) # wait for message delivery
    @info "ctx: $ctx"

    # When a component is not a mqtt component like mosca above
    # the subscribers receive two pubsub messages.
    @test ctx["count"] == 3
    @test ctx["msg"] == "hello from publisher"

    # a multiple args message is not delivered to MQTT
    Rembus.publish(pub, "mytopic", "buonanotte", 100)

end

@info "[mqtt_publish] start"
try
    run()
catch e
    @test false
    @error "[server error: $e]"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[mqtt:publish] stop"
