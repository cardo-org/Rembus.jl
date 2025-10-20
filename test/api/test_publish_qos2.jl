include("../utils.jl")

foo(ctx, rb, x) = ctx[rid(rb)] = x

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
    ctx = Dict()

    sub = connect(Rembus.RbURL(sub_url), name="subscriber")
    @test isnothing(subscribe(sub, foo))
    inject(sub, ctx)
    reactive(sub)

    pub = connect(Rembus.RbURL(pub_url), name="publisher")
    val = 1

    Rembus.reset_probe!(sub)
    Rembus.reset_probe!(pub)

    publish(pub, "foo", val, qos=Rembus.QOS2)

    @test wait_message() do
        haskey(ctx, rid(sub)) && ctx[rid(sub)] == val
    end
    sleep(0.2)

    @info "[publish_qos2] shutting down"
    shutdown(sub)
    shutdown(pub)
    sub_messages = Rembus.probe_inspect(sub)
    @info "[$sub] PROBE: $sub_messages"
    Rembus.probe_pprint(sub)
    Rembus.probe_pprint(pub)
    @test sub_messages[1].direction === Rembus.pktin
    @test isa(sub_messages[1].msg, Rembus.PubSubMsg)
    @test sub_messages[2].direction === Rembus.pktout
    @test isa(sub_messages[2].msg, Rembus.AckMsg)

    index = isa(sub_messages[3].msg, Rembus.Ack2Msg) ? 3 : 4
    @test sub_messages[index].direction === Rembus.pktin
    @test isa(sub_messages[index].msg, Rembus.Ack2Msg)

    return ctx
end

@info "[publish_qos2] start"
try
    #pub_url = "ws://127.0.0.1:8010/pub"
    #sub_url = "ws://127.0.0.1:8010/sub"
    pub_url = "zmq://127.0.0.1:8012/pub"
    sub_url = "zmq://127.0.0.1:8012/sub"

    rb = broker(ws=8010, zmq=8012)
    ctx = run(pub_url, sub_url)
    @test ctx["sub"] == 1
catch e
    @error "[publish_qos2] error: $e"
    @test false
finally
    shutdown()
end
@info "[publish_qos2] end"
