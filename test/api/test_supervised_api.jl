include("../utils.jl")

my_topic(ctx) = @info "my_topic called"
my_service(ctx, x, y) = x + y

function run()
    ctx = "mystring"

    rb = component("abc")

    shared(rb, ctx)
    subscribe(rb, my_topic, true)
    subscribe(rb, "topic", my_topic, true)
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

    @test_throws RpcMethodUnavailable rpc(client, "service", [1, 2])

    # just for lines coverage
    @async forever(rb)

    publish(rb, "devnull")
    res = rpc(rb, "version")
    @test isa(res, String)

    close(client)
    terminate(rb)
end

execute(run, "test_supervised_api")
