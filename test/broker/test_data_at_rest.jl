include("../utils.jl")

using DataFrames
using Dates

broker_name = "data_at_rest"
rounds = 10

mutable struct Ctx
    count::Int
    Ctx() = new(0)
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

function run(pub_url, sub_url)
    ctx = Ctx()
    bro = broker(name=broker_name, ws=8010)
    Rembus.islistening(bro, protocol=[:ws], wait=10)

    sub = connect(Rembus.RbURL(sub_url), name="saved_messages_sub")
    @test isnothing(subscribe(sub, foo, Rembus.LastReceived))
    inject(sub, ctx)
    reactive(sub)

    pub = connect(Rembus.RbURL(pub_url), name="saved_messages_pub")
    count = 0

    while count < rounds
        publish(pub, "foo_qos2", Dict(string(count) => Float32(count)), qos=Rembus.QOS2)
        publish(pub, "foo_qos0", count, qos=Rembus.QOS0)
        count += 1
    end
    sleep(1)
    close(pub)
    close(sub)
end


@info "[data_at_rest] start"
try
    pub_url = "ws://127.0.0.1:8010/pub"
    sub_url = "ws://127.0.0.1:8010/sub"

    request_timeout!(20)

    run(pub_url, sub_url)
catch e
    @error "[data_at_rest] error: $e"
    @test false
finally
    shutdown()
end

df = Rembus.data_at_rest(broker=broker_name, from=Dates.Second(2))
@test nrow(df) == rounds * 2 + 4 # two connection_up and two connection_down
@info "[data_at_rest] end"
