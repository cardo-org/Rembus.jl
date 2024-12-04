include("../utils.jl")

function mytopic(ctx, rb)
    ctx.count += 1
end

function consume()
end

mutable struct Ctx
    count::UInt
    Ctx() = new(0)
end

function run()
    ctx = Ctx()
    s1 = server(ws=8000)
    s2 = server(ws=8001)
    subscribe(s1, mytopic)
    subscribe(s2, mytopic)
    inject(s1, ctx)
    inject(s2, ctx)

    Rembus.islistening(wait=20, procs=["server.serve:8000", "server.serve:8001"])

    c = component(["ws://:8000", "ws://:8001"])

    while (!isconnected(c))
        sleep(0.1)
    end
    sleep(1)

    subscribe(c, consume)

    result = rpc(c, "version")
    @info "[test_component_tomany] result=$result"

    publish(c, "mytopic")
    sleep(0.2)
    @info "[test_component_tomany] count: $(ctx.count)"
    @test ctx.count == 2

    fut = rpc(c, "version", wait=false)
    result = fetch_response(fut)
    @test result == Rembus.VERSION
end

@info "[test_component_tomany] start"
try
    run()
catch e
    @error "[test_component_tomany] unexpected error: $e"
    @test false
finally
    shutdown()
end
@info "[test_component_tomany] stop"
