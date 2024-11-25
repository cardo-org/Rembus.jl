include("../utils.jl")

# The Server node is a subscriber

function sub_egress(rb, msg)
    response = msg
    @info "[$rb] egress: $(msg.id) ($(typeof(msg)))"
    if isa(msg, Rembus.AckMsg)
        rb.router.shared.msgid["sub_ack"] = msg.id
    end

    return response
end

function sub_ingress(rb, msg)
    response = msg
    @info "[$rb] ingress: $(msg.id) ($(typeof(msg)))"
    if isa(msg, Rembus.PubSubMsg)
        rb.router.shared.msgid["sub_pubsub"] = msg.id
    elseif isa(msg, Rembus.Ack2Msg)
        rb.router.shared.msgid["sub_ack2"] = msg.id
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
    srv = server(ctx)
    egress_interceptor(srv, sub_egress)
    ingress_interceptor(srv, sub_ingress)
    subscribe(srv, topic, msg_handler)
    return srv
end

function run()
    ctx = Ctx()
    topic = "qos2_topic"
    rb = sub(topic, ctx)
    pub_rb = pub(topic, ctx)

    sleep(2)
    @info "events: $ctx"
    @test ctx.msgid["pub_ack"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_pubsub"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_ack"] == ctx.msgid["pub_pubsub"]
    @test ctx.msgid["sub_ack2"] == ctx.msgid["pub_pubsub"]

    terminate(rb)
    close(pub_rb)
end

# cleanup files
rm(joinpath(Rembus.rembus_dir(), "sub.db"), force=true)

@info "[test_server_qos2] start"
try
    run()
catch e
    @error "[test_server_qos2] unexpected error: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
end
@info "[test_server_qos2] stop"

if !Sys.iswindows()
    # expect one messages at rest
    df = Rembus.data_at_rest(string(1), BROKER_NAME)
    @test nrow(df) == 1
end
