using Mosquitto
using Rembus
using Test

"""
    wait_for_message(ctx::Dict, key::String; timeout::Float64=5.0, interval::Float64=0.1)

Wait until `ctx[key]` becomes nonzero (meaning a message arrived),
or throw an error if nothing arrives within `timeout` seconds.

Useful for MQTT tests in CI where broker or subscriber might be slow.
"""
function wait_for_message(ctx::Dict, key::String; timeout::Float64=5.0, interval::Float64=0.1)
    start = time()
    while (time() - start) < timeout
        if get(ctx, key, 0) > 0
            return
        end
        sleep(interval)
    end
    error("Timeout: no message received for key '$key' within $timeout seconds")
end

function mytopic(ctx, rb, msg)
    @info "[mytopic] got message: $msg"
    ctx["msg"] = msg
    ctx["count"] += 1
end

function run()
    host = get(ENV, "MQTT_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "MQTT_PORT", "1883"))

    ctx = Dict{String,Any}("count" => 0)

    broker = component("mqtt://$host:$port/mosca", ws=8000)

    # wait for connection establishement
    @test Rembus.islistening(broker, wait=10)

    rb = component("mqtt_subscriber")
    inject(rb, ctx)
    Rembus.subscribe(rb, mytopic)
    reactive(rb)


    Rembus.publish(broker, "mytopic", "hello mosca")
    wait_for_message(ctx, "count"; timeout=5.0)

    @info "ctx: $ctx"
    @test ctx["count"] == 1
    @test ctx["msg"] == "hello mosca"

    ctx["count"] = 0
    pub = component("mqtt_publisher")
    Rembus.publish(pub, "mytopic", "hello from publisher")
    wait_for_message(ctx, "count"; timeout=5.0)
    @info "ctx: $ctx"

    # When a component is not a mqtt component like mosca above
    # the subscribers receive two pubsub messages.
    @test ctx["count"] == 2
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
