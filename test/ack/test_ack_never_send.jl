include("../utils.jl")

using Dates

function Rembus.transport_send(socket::Rembus.WS, msg::Rembus.AckMsg)
    #@info "[$(now())][never_send] pretending to send: $msg"
    return true
end

messages = 10

mutable struct Ctx
    data1::Int
    data2::Int
    Ctx() = new(0, 0)
end

function foo(ctx, rb, x)
    @debug "foo recv: $x"
    if x == "data1"
        ctx.data1 += 1
    elseif x == "data2"
        ctx.data2 += 1
    end
end

function run(pub_url, sub_url)
    ctx = Ctx()

    sub = connect(Rembus.RbURL(sub_url), name="saved_messages_sub")
    @test isnothing(subscribe(sub, foo, Rembus.LastReceived))
    inject(sub, ctx)
    reactive(sub)

    pub = connect(Rembus.RbURL(pub_url), name="saved_messages_pub")
    pub.router.settings.send_retries = 0
    pub.router.settings.ack_timeout = 0.5

    publish(pub, "foo", "data1", qos=Rembus.QOS1)
    publish(pub, "foo", "data2", qos=Rembus.QOS1)

    sleep(5)
    @debug "count: $ctx"

    # the pub component does not retry to send the messages,
    # so the sub should receive 1+setting.send_retries messages
    # from the broker and the default send_retries equals 3
    @test ctx.data1 == 4
    @test ctx.data2 == 4
end


@info "[ack_never_send] start"
try
    pub_url = "ws://127.0.0.1:8010/pub"
    sub_url = "ws://127.0.0.1:8010/sub"

    request_timeout!(20)
    rb = broker(ws=8010, name="ack_never_send")
    rb.router.settings.ack_timeout = 0.5
    run(pub_url, sub_url)
catch e
    @error "[ack_never_send] error: $e"
    @test false
finally
    shutdown()
end
@info "[ack_never_send] end"
