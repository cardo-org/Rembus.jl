include("../utils.jl")

mutable struct Ctx
    data::Any
end

function atopic(ctx, rb, x)
    @info "[test_component] atopic: $x"
    ctx.data = x
end

function myservice()
    @info "[test_component] myservice requested"
    return 1
end

function run()

    rb = component("foo")

    # first make a request
    res = rpc(rb, "version", wait=false)
    @info "response=$res"

    # then start the broker
    bro = broker(wait=false, ws=8000, tcp=8001)
    islistening(bro, protocol=[:ws, :tcp])

    # and get the response
    ver = fetch_response(res)
    @test ver === Rembus.VERSION

    for fn in [expose, unexpose, subscribe, unsubscribe]
        res = fn(rb, atopic, wait=false)
        @test fetch_response(res) === nothing
    end

    res = reactive(rb, wait=false)
    @test fetch_response(res) === nothing

    res = unreactive(rb, wait=false)
    @test fetch_response(res) === nothing

    res = expose(rb, "my_cool_service", myservice, wait=false)
    @test fetch_response(res) === nothing

    res = unexpose(rb, "my_cool_service", wait=false)
    @test fetch_response(res) === nothing

    shutdown(rb)

    rb = component(["ws://:8000", "tcp://:8001"])
    all_policy(rb)

    futures = rpc(rb, "version", wait=false)
    values = fetch_response(futures)
    @test values == [Rembus.VERSION, Rembus.VERSION]

    # test rpcreq broadcast flag
    expose(rb, myservice, wait=false)

    shutdown(rb)

    rb = component(["ws://:8000", "tcp://:8003"])
    all_policy(rb)

    futures = rpc(rb, "version", wait=false)
    values = fetch_response(futures)
    @test values[1] === Rembus.VERSION
    @test values[2] === missing

    shutdown(rb)

    rb = connect(["ws://:8005", "ws://:8006"])
    future = rpc(rb, "version", wait=false)
    @test_throws RembusError fetch_response(future)
end

@info "[test_component] start"

try
    run()

catch e
    @error "[test_component] unexpected error: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
end
@info "[test_component] stop"
