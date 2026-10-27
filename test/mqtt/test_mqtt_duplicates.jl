using Mosquitto
using Rembus
using Test

# When the MQTT bridge is attached, a message published by a non-MQTT
# component is published to the MQTT broker and echoed back by it: the echo
# must be dropped so that a subscriber receives the message only once.

function dup_topic(msg; ctx, node)
    push!(ctx["msgs"], msg)
end

function empty_topic(; ctx, node)
    ctx["empty"] += 1
end

function run()
    host = get(ENV, "MQTT_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "MQTT_PORT", "1883"))

    ctx = Dict{String,Any}("msgs" => Any[], "empty" => 0)

    broker = component("mqtt://$host:$port/mosca_dup", ws=8338)
    @test Rembus.islistening(broker, wait=10)

    sub = component("mqtt_dup_subscriber")
    inject(sub, ctx)
    Rembus.subscribe(sub, dup_topic)
    Rembus.subscribe(sub, empty_topic)
    reactive(sub)

    # let the MQTT subscription settle
    sleep(2)

    pub = component("mqtt_dup_publisher")
    Rembus.publish(pub, "dup_topic", "hello")

    # wait for the first copy, then give the MQTT copy time to arrive
    timeout = time() + 5.0
    while isempty(ctx["msgs"]) && time() < timeout
        sleep(0.1)
    end
    sleep(2)

    @test length(ctx["msgs"]) == 1
    @test ctx["msgs"][1] == "hello"

    # a message without arguments is echoed too (empty payload)
    Rembus.publish(pub, "empty_topic")
    sleep(3)
    @test ctx["empty"] == 1

    # a message published by an external MQTT client is not an echo
    client = Mosquitto.Client_v5(host, port)
    Mosquitto.loop_start(client)
    Mosquitto.publish(client, "dup_topic", "\"external\"")
    timeout = time() + 5.0
    while length(ctx["msgs"]) < 2 && time() < timeout
        sleep(0.1)
    end
    sleep(1)
    @test ctx["msgs"] == ["hello", "external"]
    Mosquitto.disconnect(client)
    Mosquitto.loop_stop(client)
end

@info "[mqtt_duplicates] start"
try
    run()
catch e
    @test false
    @error "[mqtt_duplicates] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[mqtt_duplicates] stop"
