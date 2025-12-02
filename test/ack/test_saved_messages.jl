include("../utils.jl")

using Preferences

messages = 3

mutable struct Ctx
    count::Int
    bar_count::Int
    Ctx() = new(0, 0)
end

function bar(x; ctx, node)
    @info "bar recv: $x"
    ctx.bar_count += 1
end

function foo(x; ctx, node)
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

function produce(pub_url)
    pub = connect(Rembus.RbURL(pub_url), name="saved_messages_pub")

    count = 0
    while count < messages
        publish(pub, "foo", count, qos=Rembus.QOS2)
        publish(pub, "bar", count)
        count += 1
    end
    sleep(1)
    shutdown(pub)
end

function consume(sub_url)
    ctx = Ctx()

    sub = connect(Rembus.RbURL(sub_url), name="saved_messages_sub")
    @test isnothing(subscribe(sub, foo, Rembus.LastReceived))
    @test isnothing(subscribe(sub, bar, Rembus.LastReceived))
    inject(sub, ctx)
    reactive(sub, 1_900_000)

    @test wait_message() do
        ctx.count == messages
    end

    #    @test wait_message() do
    #        ctx.bar_count == 1
    #    end

    @info "[saved_messages] shutting down"
    shutdown(sub)
end


@info "[saved_messages] start"
try
    pub_url = "ws://127.0.0.1:8010/pub"
    sub_url = "ws://127.0.0.1:8010/sub"

    ENV["REMBUS_CACHE_SIZE"] = "2"

    request_timeout!(20)
    rb = broker(ws=8010, name="saved_messages")
    @info "cache size: $(rb.router.settings.cache_size)"
    produce(pub_url)
    consume(sub_url)
catch e
    @error "[saved_messages] error: $e"
    @test false
finally
    shutdown()
end
@info "[saved_messages] end"
