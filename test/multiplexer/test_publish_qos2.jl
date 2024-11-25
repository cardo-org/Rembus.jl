include("../utils.jl")

function mytopic(ctx, rb)
    @info "[test_publish_qos2]: message mytopic received"
    ctx.count += 1
end

function start_servers(ctx)
    s1 = server(ctx, ws=5000)
    subscribe(s1, mytopic)
end

mutable struct Ctx
    count::Int
end

function run()
    ctx = Ctx(0)
    rb = component(["ws://:5000", "ws://:5001"])
    isconn = isconnected(rb)
    @info "isconnected: $isconn"

    publish(rb, "mytopic", qos=QOS2)

    sleep(1)
    start_servers(ctx)

    sleep(2)

    terminate(rb)
    @test ctx.count == 1
end

@info "[test_publish_qos2] start"
try
    mkpath(Rembus.rembus_dir())
    run()
catch e
    @error "[test_publish_qos2] error: $e"
    @test false
finally
    shutdown()
end

@info "[test_publish_qos2] stop"
