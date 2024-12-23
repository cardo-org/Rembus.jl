include("../utils.jl")

function sub_egress(rb, msg)
    response = msg
    if isa(msg, Rembus.AckMsg)
        rb.shared.msgid["sub_ack"] = msg.id
    end

    return response
end

function sub_ingress(rb, msg)
    response = msg
    if isa(msg, Rembus.PubSubMsg)
        rb.shared.msgid["sub_pubsub"] = msg.id
    elseif isa(msg, Rembus.Ack2Msg)
        rb.shared.msgid["sub_ack2"] = msg.id
        # to simulate an ACK2 message lost
        return nothing
    end
    return response
end

function pub_ingress(rb, msg)
    response = msg
    if isa(msg, Rembus.AckMsg)
        rb.shared.msgid["pub_ack"] = msg.id
    end
    return response
end

function pub_egress(rb, msg)
    response = msg
    if isa(msg, Rembus.PubSubMsg)
        rb.shared.msgid["pub_pubsub"] = msg.id
    end

    return response
end

function msg_handler(ctx, rb, counter)
    @info "recv message: $counter"
end

function pub(topic, ctx)
    pub = connect("pub")
    inject(pub, ctx)
    egress_interceptor(pub, pub_egress)
    ingress_interceptor(pub, pub_ingress)
    publish(pub, topic, 1, qos=QOS2)
    return pub
end

mutable struct Ctx
    msgid::Dict
    Ctx() = new(Dict())
end

function sub(topic, ctx)
    sub = connect("sub")
    inject(sub, ctx)
    egress_interceptor(sub, sub_egress)
    ingress_interceptor(sub, sub_ingress)
    subscribe(sub, topic, msg_handler)
    reactive(sub)
    return sub
end

function run()
    ctx = Ctx()
    topic = "qos2_topic"
    rb = sub(topic, ctx)
    pub_rb = pub(topic, ctx)

    sleep(1)
    @info "events: $ctx"
    @test ctx.msgid["pub_ack"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_pubsub"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_ack"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_ack2"] == ctx.msgid["pub_pubsub"]

    df = Rembus.awaiting_ack2(rb)
    @info "messages awaiting ack2:\n$df"
    close(rb)
    close(pub_rb)
    @test nrow(df) == 1
end

# cleanup files
rm(joinpath(Rembus.rembus_dir(), "sub.acks"), force=true)

execute(run, "test_qos2")

df = Rembus.load_pubsub_received(Rembus.RbURL("sub"))
@test !isempty(df)

if !Sys.iswindows()
    # expect one messages at rest
    df = Rembus.data_at_rest(string(1), BROKER_NAME)
    @test nrow(df) == 1
end
