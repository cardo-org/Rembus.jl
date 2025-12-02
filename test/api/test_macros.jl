include("../utils.jl")

Rembus.info!()

mytopic(k, v; ctx, node) = ctx[k] = v

myservice(x, y; ctx, node) = x + y

client_service(x, y; ctx, node) = x * y
client_topic(k, v; ctx, node) = ctx[k] = v

function run()
    key = "mykey"
    value = 100

    ctx = Dict()
    client_ctx = Dict()

    rb = connect()
    inject(rb, client_ctx)
    expose(rb, client_service)
    subscribe(rb, client_topic)
    reactive(rb)

    @inject ctx
    @subscribe mytopic
    @expose myservice

    @expose bar(a, b; ctx, node) = a - b
    @subscribe zoo(a, b, c; ctx, node) = @info "zoo recv: $a,$b,$c "

    @reactive

    publish(rb, "mytopic", key, value)
    publish(rb, "zoo", 1, 2, 3)

    result = rpc(rb, "myservice", 1, 1)
    @test result == 2

    @test rpc(rb, "bar", 10, 1) == 10 - 1

    check_sentinel(ctx, sentinel=key)

    result = @rpc client_service(2, 3)
    @test result == 2 * 3

    @publish client_topic(key, value)
    check_sentinel(client_ctx, sentinel=key)

    @unreactive
    @unsubscribe mytopic
    @unexpose myservice
    @info "ctx: $ctx"
end

execute(run, "macros")
