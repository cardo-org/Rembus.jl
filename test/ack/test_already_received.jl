include("../utils.jl")

function Rembus.transport_send(socket::Rembus.WS, msg::Rembus.Ack2Msg)
    return true
end

messages = 10

mutable struct Ctx
    count::Int
    Ctx() = new(0)
end

function foo(ctx, rb, x)
    @info "foo recv: $x"
    ctx.count += 1
end

function wait_message(fn, max_wait=10)
    wtime = 0.1
    t = 0
    while t < max_wait
        t += wtime
        sleep(wtime)
        if fn()
            return true
        end
    end
    return false
end

function run(pub_url, sub_url)
    ctx = Ctx()

    sub = connect(sub_url, name="saved_messages_sub")
    @test isnothing(subscribe(sub, foo, Rembus.LastReceived))
    inject(sub, ctx)
    reactive(sub)

    pub = connect(pub_url, name="saved_messages_pub")
    count = 0

    msg = Rembus.PubSubMsg(pub, "foo", "data", Rembus.QOS2)
    @info "[already_received] msgid: $([msg.id])"
    while count < messages
        #publish(pub, "foo", count, qos=Rembus.QOS2)
        msg.counter = Rembus.save_message(pub.router, msg)
        Rembus.message_send(pub, msg)
        count += 1
    end

    sleep(2)
    @info "count: $(ctx.count)"
    @test ctx.count == 1
    @info "[already_received] shutting down"
end


@info "[already_received] start"
try
    pub_url = "ws://127.0.0.1:8010/pub"
    sub_url = "ws://127.0.0.1:8010/sub"

    Rembus.request_timeout!(20)
    rb = broker(ws=8010, name="already_received")
    run(pub_url, sub_url)
catch e
    @error "[already_received] error: $e"
    @test false
finally
    shutdown()
end
@info "[already_received] end"
