using Mosquitto
using Rembus
using Test

# Requires a Mosquitto broker with a TLS listener whose certificate is signed
# by the CA in MQTTS_CAFILE (see .github/workflows/CI.yml). The test is skipped
# if the CA file is not available.

function wait_for(cond; timeout::Float64=10.0, interval::Float64=0.1)
    start = time()
    while (time() - start) < timeout
        cond() && return true
        sleep(interval)
    end
    return cond()
end

function mytopic(args...; ctx, node)
    ctx["args"] = collect(args)
    ctx["count"] += 1
end

function run(cafile)
    host = get(ENV, "MQTTS_HOST", "127.0.0.1")
    port = parse(Int, get(ENV, "MQTTS_PORT", "8883"))

    ctx = Dict{String,Any}("count" => 0)

    # The mqtts transport takes the CA from the Rembus configuration.
    withenv("HTTP_CA_BUNDLE" => cafile) do
        broker = component("mqtts://$host:$port/mosca_tls", ws=8338)
        @test Rembus.islistening(broker, wait=10)

        rb = component("mqtts_subscriber")
        inject(rb, ctx)
        Rembus.subscribe(rb, mytopic)
        reactive(rb)
        sleep(2)

        # Rembus -> MQTT over TLS -> Rembus
        Rembus.publish(broker, "mytopic", "hello tls")
        @test wait_for(() -> ctx["count"] > 0)
        @test ctx["args"] == ["hello tls"]

        # A plain MQTT client publishing over TLS.
        ctx["count"] = 0
        client = Mosquitto.Client_v5()
        @test Mosquitto.tls_set(client, cafile) == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS
        Mosquitto.connect(client, host, port)
        Mosquitto.loop_start(client)
        Mosquitto.publish(client, "mytopic", "{\"a\":1}")
        @test wait_for(() -> ctx["count"] > 0)
        @test ctx["args"] == [Dict("a" => 1)]
        Mosquitto.disconnect(client)
        Mosquitto.loop_stop(client)
    end
end

@info "[mqtts_publish] start"
cafile = get(ENV, "MQTTS_CAFILE", "")
if isfile(cafile)
    try
        run(cafile)
    catch e
        @test false
        @error "[mqtts_publish] server error: $e"
        showerror(stdout, e, catch_backtrace())
    finally
        shutdown()
    end
else
    @info "[mqtts_publish] skipped: MQTTS_CAFILE not set or not found"
end
@info "[mqtts_publish] stop"
