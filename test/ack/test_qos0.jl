include("../utils.jl")

function sub_egress(rb, msg)
    response = msg
    @info "[$rb] egress: $(msg.id) ($(typeof(msg)))"
    if isa(msg, Rembus.AckMsg)
        rb.shared.msgid["sub_ack"] = msg.id
    end

    return response
end

function sub_ingress(rb, msg)
    response = msg
    @info "[$rb] ingress: $(msg.id) ($(typeof(msg)))"
    if isa(msg, Rembus.PubSubMsg)
        rb.shared.msgid["sub_pubsub"] = msg.id
    end
    return response
end

function pub_ingress(rb, msg)
    response = msg
    @info "[$rb] ingress: $(msg.id) ($(typeof(msg)))"
    if isa(msg, Rembus.AckMsg)
        rb.shared.msgid["pub_ack"] = msg.id
    end
    return response
end

function pub_egress(rb, msg)
    response = msg
    @info "[$rb] egress: $(msg.id) ($(typeof(msg)))"

    if isa(msg, Rembus.PubSubMsg)
        rb.shared.msgid["pub_pubsub"] = msg.id
    end

    return response
end

function msg_handler(ctx, counter)
    @info "recv message: $counter"
end

function pub(topic, ctx)
    pub = connect("pub")
    shared(pub, ctx)
    egress_interceptor(pub, pub_egress)
    ingress_interceptor(pub, pub_ingress)
    publish(pub, topic, 1, qos=QOS0)
    return pub
end

mutable struct Ctx
    msgid::Dict
    Ctx() = new(Dict())
end

function sub(topic, ctx)
    sub = connect("sub")
    shared(sub, ctx)
    egress_interceptor(sub, sub_egress)
    ingress_interceptor(sub, sub_ingress)
    subscribe(sub, topic, msg_handler)
    reactive(sub)
    return sub
end

function run()
    ctx = Ctx()
    topic = "qos0_topic"
    rb = sub(topic, ctx)
    pub_rb = pub(topic, ctx)

    sleep(1)
    @info "events: $ctx"
    @test ctx.msgid["pub_pubsub"] == 0
    @test ctx.msgid["sub_pubsub"] == 0
    @test !haskey(ctx.msgid, "pub_ack")
    @test !haskey(ctx.msgid, "sub_ack")

    close(rb)
    close(pub_rb)
end

execute(run, "test_qos2")

# expect one messages at rest
df = Rembus.data_at_rest(string(1), BROKER_NAME)
@test nrow(df) == 1
