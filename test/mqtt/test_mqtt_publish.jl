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

function wait_for_value(ctx::Dict, key::String, expected; timeout::Float64=5.0, interval::Float64=0.1)
    start = time()
    while (time() - start) < timeout
        get(ctx, key, nothing) == expected && return
        sleep(interval)
    end
    return
end

function mytopic(msg; ctx, node)
    @info "[mytopic] got message: $msg"
    ctx["msg"] = msg
    ctx["count"] += 1
end

function mytopic2(args...; ctx, node)
    ctx["args"] = collect(args)
    ctx["count2"] += 1
end

function run()
    host = get(ENV, "MQTT_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "MQTT_PORT", "1883"))

    ctx = Dict{String,Any}("count" => 0, "count2" => 0)

    broker = component("mqtt://$host:$port/mosca", ws=8338)

    # wait for connection establishement
    @test Rembus.islistening(broker, wait=10)

    rb = component("mqtt_subscriber")
    inject(rb, ctx)
    Rembus.subscribe(rb, mytopic)
    reactive(rb)

    sleep(2)

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

    # The MQTT echo of a message published through the bridge is dropped:
    # the subscribers receive a single pubsub message.
    sleep(2)
    @test ctx["count"] == 1
    @test ctx["msg"] == "hello from publisher"

    # a multiple args message is published as a JSON array and the
    # elements of the array received from MQTT become the message arguments.
    Rembus.subscribe(rb, mytopic2)
    sleep(1)
    Rembus.publish(pub, "mytopic2", "buonanotte", 100)
    wait_for_message(ctx, "count2"; timeout=5.0)
    @test ctx["args"] == ["buonanotte", 100]

    # a JSON object from MQTT is a single argument
    ctx["count2"] = 0
    client = Mosquitto.Client_v5(host, port)
    Mosquitto.loop_start(client)
    Mosquitto.publish(client, "mytopic2", "{\"a\":1}")
    # late duplicates of the previous message may still arrive: wait for the
    # expected value instead of the first message.
    wait_for_value(ctx, "args", [Dict("a" => 1)]; timeout=10.0)
    @test ctx["args"] == [Dict("a" => 1)]

    # a not JSON payload is dropped
    ctx["count2"] = 0
    Mosquitto.publish(client, "mytopic2", "not json")
    sleep(1)
    @test ctx["count2"] == 0
    Mosquitto.disconnect(client)
    Mosquitto.loop_stop(client)

end

@info "[mqtt_publish] start"
try
    run()
catch e
    @test false
    @error "[mqtt_publish] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[mqtt_publish] stop"
