include("../utils.jl")

using DataFrames
using Dates

broker_name = "setup"

Rembus.info!()

mutable struct Ctx
    count::Int
    Ctx() = new(0)
end

function consumer(ctx, rb, x)
    @info "consumer recv: $x"
    ctx.count += 1
end

service1(x, y) = x + y
service2(x, y) = x * y

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

function twin_setup(router, twin)
    cfg::Dict{String,Any} = Dict(
        "subscribers" => ["consumer"], "exposers" => ["service1", "service2"]
    )
    cfg[Rembus.COMMAND] = Rembus.SETUP_CMD
    msg = Rembus.AdminReqMsg(
        twin,
        Rembus.BROKER_CONFIG,
        cfg,
        tid(twin)
    )
    response = Rembus.send_msg(twin, msg)
    result = Rembus.fetch(response.future)
    close(response.timer)
    return (result.status === Rembus.STS_SUCCESS)
end

function run(pub_url, sub_url)
    ctx = Ctx()
    bro = broker(name=broker_name, ws=8010)
    Rembus.islistening(bro, protocol=[:ws], wait=10)

    # start a  listener to test update_tables()
    sub = component(sub_url, ws=10000)
    @test twin_setup(sub.router, sub)

    @test haskey(bro.router.topic_impls, "service1")
    @test haskey(bro.router.topic_impls, "service2")
    @test haskey(bro.router.topic_interests, "consumer")

    exports = [["myservice1", "myservice2"], ["mytopic1", "mytopic2"]]
    Rembus.update_tables(sub.router, sub, exports)
    @test haskey(sub.router.topic_impls, "myservice1")
    @test haskey(sub.router.topic_interests, "mytopic2")
end

@info "[setup] start"
try
    pub_url = "ws://127.0.0.1:8010/setup_pub"
    srv_url = "ws://127.0.0.1:8010/setup_srv"

    # clear configurartion
    rm(joinpath(Rembus.rembus_dir(), broker_name), recursive=true, force=true)

    Rembus.request_timeout!(20)
    run(pub_url, srv_url)
catch e
    @error "[setup] error: $e"
    @test false
finally
    shutdown()
end
@info "[setup] end"
