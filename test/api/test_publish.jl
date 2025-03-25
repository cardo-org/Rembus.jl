include("../utils.jl")

using Dates

function foo(ctx, rb, x)
    ctx[rid(rb)] = x
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
    ctx = Dict()

    sub = connect(sub_url)

    # Test empty probed messages
    @test isempty(Rembus.probe_inspect(sub))

    @info "[$sub_url] socket: $(sub.socket)"
    @test isnothing(subscribe(sub, foo, Dates.CompoundPeriod(Day(1), Minute(1))))
    inject(sub, ctx)
    reactive(sub, Rembus.Now)

    pub = connect(pub_url)
    Rembus.failover_queue!(pub, "foo")
    @test Rembus.failover_queue(pub)

    val = 1

    Rembus.reset_probe!(sub)
    Rembus.reset_probe!(pub)
    publish(pub, "foo", val, qos=Rembus.Rembus.QOS2)

    @test wait_message() do
        haskey(ctx, rid(sub)) && ctx[rid(sub)] == val
    end
    sleep(0.1)
    unsubscribe(sub, foo)

    # This message is not delivered to sub because sub unsubscribed.
    publish(pub, "foo", 2 * val)

    @test wait_message() do
        haskey(ctx, rid(sub)) && ctx[rid(sub)] == val
    end

    Rembus.unprobe!(sub)

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

@info "[test_publish] start"
try
    #pub_url = "tcp://:8011/pub"
    #sub_url = "tcp://:8011/sub"
    #pub_url = "ws://:8010/pub"
    #sub_url = "ws://:8010/sub"
    #pub_url = "zmq://:8012/pub"
    #sub_url = "zmq://:8012/sub"

    rb = broker(ws=8010, tcp=8011, zmq=8012, prometheus=7071, name="publish")
    @test Rembus.islistening(rb, wait=10)
    for pub_url in [
        "tcp://127.0.0.1:8011/publish_tcppub",
        "ws://127.0.0.1:8010/publish_pub"
    ]
        for sub_url in [
            "tcp://127.0.0.1:8011/publish_tcpsub",
            "ws://127.0.0.1:8010/publish_wssub",
            "zmq://127.0.0.1:8012/publish_zmqsub"
        ]
            ctx = run(pub_url, sub_url)
            url = Rembus.RbURL(sub_url)
            @test ctx[rid(url)] == 1
        end
    end

    shutdown(rb)
    if Base.Sys.iswindows()
        @info "Windows platform detected: skipping test_https"
    else
        # create keystore
        test_keystore = joinpath(tempdir(), "keystore")
        script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
        ENV["REMBUS_KEYSTORE"] = test_keystore
        ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
        try
            Base.run(`$script -k $test_keystore`)
            rb = broker(secure=true, ws=6010, tcp=6011, zmq=6012, name="publish")
            @test Rembus.islistening(rb, wait=10)
            for pub_url in [
                "tls://127.0.0.1:6011/publish_pub",
                "wss://127.0.0.1:6010/publish_pub"
            ]
                for sub_url in [
                    "tls://127.0.0.1:6011/publish_tcpsub",
                    "wss://127.0.0.1:6010/publish_wssub"
                ]
                    ctx = run(pub_url, sub_url)
                    url = Rembus.RbURL(sub_url)
                    @test ctx[rid(url)] == 1
                end
            end
        finally
            delete!(ENV, "REMBUS_KEYSTORE")
            delete!(ENV, "HTTP_CA_BUNDLE")
            rm(test_keystore, recursive=true, force=true)
        end
    end
catch e
    @error "[test_publish] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_publish] end"
