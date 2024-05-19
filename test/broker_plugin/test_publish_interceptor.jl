using Rembus
using Test

module CarontePlugin

using Rembus

function republish(router, component, topic, data, flags=UInt8(0))
    new_msg = Rembus.PubSubMsg(topic, data, flags)
    put!(router.process.inbox, Rembus.Msg(Rembus.TYPE_PUB, new_msg, component))
end

function get_payload(io::IOBuffer)
    mark(io)
    payload = decode(io)
    @info "payload=$payload"
    reset(io)
    return payload
end

#function published(broker, component, msg)
function publish_interceptor(broker, component, msg)
    @info "[$component]: pub: $msg ($(msg.data))"

    subjects = split(msg.topic, "/")
    metric = last(subjects)

    content = Dict()
    content["location"] = msg.topic
    content["value"] = get_payload(msg.data)

    republish(broker, component, metric, content)

    # do not broadcast original message
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

function run()
    ctx = Ctx()

    Rembus.set_broker_plugin(CarontePlugin)
    caronte(wait=false)

    pub = connect()
    sub = connect()
    shared(sub, ctx)
    subscribe(sub, temperature)
    reactive(sub)

    publish(pub, "town/house/kitchen/temperature", 20.2)
    publish(pub, "town/house/garden/temperature", 25.2)

    sleep(1)
    close(pub)
    close(sub)
    @info "ctx: $ctx"
    @test length(ctx.received) == 2
    @test ctx.received["town/house/kitchen/temperature"] == 20.2
    @test ctx.received["town/house/garden/temperature"] == 25.2
    shutdown()
end

@info "[test_publish_interceptor] start"
run()
@info "[test_publish_interceptor] stop"
