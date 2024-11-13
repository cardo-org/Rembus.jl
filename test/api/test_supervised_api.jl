include("../utils.jl")

my_topic(ctx, rb) = @info "my_topic called"
my_service(ctx, rb, x, y) = x + y

mutable struct Ctx
    called::Bool
end

from_supervised(ctx, rb) = ctx.called = true


function run()
    ctx = "mystring"

    rb = component("abc")
    @info "component: $(rbinfo(rb))"

    shared(rb, ctx)
    subscribe(rb, my_topic, from=LastReceived())
    subscribe(rb, "topic", my_topic, from=LastReceived())
    expose(rb, "service", my_service)
    expose(rb, my_service)
    reactive(rb)

    client = connect()
    publish(client, "topic")
    res = rpc(client, "service", [1, 2])
    @test res == 3

    unsubscribe(rb, "topic")
    unexpose(rb, "service")
    unreactive(rb)

    @test_throws RpcMethodNotFound rpc(client, "service", [1, 2])

    # just for lines coverage
    @async forever(rb)

    ctx = Ctx(false)
    sub = connect()
    shared(sub, ctx)
    subscribe(sub, from_supervised)
    reactive(sub)

    publish(rb, "from_supervised")
    res = rpc(rb, "version")
    @test isa(res, String)

    close(client)
    close(sub)
    terminate(rb)
    @test ctx.called === true
end

execute(run, "test_supervised_api")
