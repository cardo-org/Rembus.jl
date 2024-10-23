include("../utils.jl")

mutable struct PluginCtx
    subscriber::Union{Nothing,Rembus.Twin}
end

module CarontePlugin
using Rembus
using Test

function subscribe_handler(ctx, broker, component, msg)
    ctx.subscriber = component
end

function publish_interceptor(ctx, component, msg)

    @info "[$component]: pub: $msg ($(msg.data))"

    subjects = split(msg.topic, "/")
    metric = last(subjects)

    data = msg_payload(msg.data)
    if isa(data, String)
        error("expected a number")
    end

    content = Dict()
    content["location"] = msg.topic
    content["value"] = data

    # publish using the internal routes
    republish(component, metric, content)

    # publish directly
    publish(ctx.subscriber, "direct_message", 999)

    # publish using router
    router = Rembus.get_router("caronte_test")
    @test router !== nothing

    publish(router, "some_topic", "some_data")

    try
        Rembus.get_router("unknown_broker")
    catch e
        @info "expected error: $e"
    end

    return false
end

end # CarontePlugin module

mutable struct Ctx
    received::Dict
    Ctx() = new(Dict())
end

function temperature(ctx, data)
    @info "[test_publish_interceptor] data:$data"
    ctx.received[data["location"]] = data["value"]
end

function direct_message(ctx, x)
    ctx.received["direct"] = x
end

function run()
    ctx = Ctx()

    caronte(
        wait=false,
        plugin=CarontePlugin,
        context=PluginCtx(nothing),
        name=BROKER_NAME
    )
    sleep(2)

    pub = connect()
    sub = connect()
    shared(sub, ctx)
    subscribe(sub, temperature)
    subscribe(sub, direct_message)

    reactive(sub)


    publish(pub, "town/house/kitchen/temperature", 20.2)
    publish(pub, "town/house/garden/temperature", 25.2)

    # publish_interceptor throws an error
    publish(pub, "town/house/garden/temperature", "foo")

    sleep(1)
    close(pub)
    close(sub)
    @info "ctx: $ctx"
    @test length(ctx.received) == 3
    @test ctx.received["town/house/kitchen/temperature"] == 20.2
    @test ctx.received["town/house/garden/temperature"] == 25.2

    @info "direct: $(ctx.received["direct"])"
    shutdown()
end

@info "[test_publish_interceptor] start"
run()
@info "[test_publish_interceptor] stop"
