include("../utils.jl")

#=
Drop an ack message to trigger a pubsub retransmission by the broker.
=#
function test_handler(rb, msg)
    response = msg
    if isa(msg, Rembus.AckMsg)
        rb.shared.ack_count += 1
        if rb.shared.ack_count == 5
            @info "dropping ack message $(rb.shared.ack_count)"
            response = nothing
        end
    end
    return response
end

function msg_handler(ctx, rb, counter)
    if haskey(ctx.recv, counter)
        ctx.recv[counter] += 1
    else
        ctx.recv[counter] = 1
    end
    @info "recv message: $counter"
end

function pub(topic, num_msg)
    pub = connect("pub")

    for i in 1:num_msg
        publish(pub, topic, i, qos=QOS2)
    end
    return pub
end

mutable struct Ctx
    ack_count::Int
    recv::Dict
    Ctx() = new(0, Dict())
end

function sub(topic, ctx)

    sub = connect("sub")

    inject(sub, ctx)
    egress_interceptor(sub, test_handler)
    subscribe(sub, topic, msg_handler)
    reactive(sub)
    return sub
end

function run(num_msg)
    ctx = Ctx()
    topic = "ack_topic"
    rb = sub(topic, ctx)
    pub_rb = pub(topic, num_msg)

    sleep(3)
    @test ctx.ack_count == num_msg + 1
    @test length(ctx.recv) == num_msg
    @test ctx.recv[UInt(5)] == 1

    close(rb)
    close(pub_rb)
end

num_msg = 6
execute(() -> run(num_msg), "test_exactly_once")

if !Sys.iswindows()
    # expect two messages at rest
    df = Rembus.data_at_rest(string(num_msg), BROKER_NAME)
    @test nrow(df) == num_msg
end
