include("../utils.jl")

# The Server node is a subscriber

function sub_egress(rb, msg)
    response = msg
    if isa(msg, Rembus.AckMsg)
        rb.router.shared.msgid["sub_ack"] = msg.id
        return nothing
    end

    return response
end

function sub_ingress(rb, msg)
    response = msg
    if isa(msg, Rembus.PubSubMsg)
        rb.router.shared.msgid["sub_pubsub"] = msg.id
    elseif isa(msg, Rembus.Ack2Msg)
        rb.router.shared.msgid["sub_ack2"] = msg.id
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

function zmq_pub(topic, ctx)
    pub = connect("zmq://:8002/pub")
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
    srv = server(ctx, ws=8000, zmq=8002)
    egress_interceptor(srv, sub_egress)
    ingress_interceptor(srv, sub_ingress)
    subscribe(srv, topic, msg_handler)
    return srv
end

function run()
    ctx = Ctx()
    topic = "client_ack_timeout_topic"
    rb = sub(topic, ctx)
    pub_rb = pub(topic, ctx)

    sleep(1)
    @info "events: $ctx"
    @test !haskey(ctx.msgid, "pub_ack")

    zmqpub_rb = zmq_pub(topic, ctx)
    sleep(1)
    @test !haskey(ctx.msgid, "pub_ack")

    shutdown(rb)

    # instead of closing set the socket to nothing
    # when this happens transport_send must stop retransmit ack packets on timeout
    pub_rb.socket = nothing

    close(zmqpub_rb)
    sleep(1)
end

# cleanup files
rm(joinpath(Rembus.rembus_dir(), "sub.db"), force=true)

@info "[test_client_ack_timeout] start"
try
    run()
catch e
    @error "[test_client_ack_timeout] unexpected error: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
end
@info "[test_client_ack_timeout] stop"
