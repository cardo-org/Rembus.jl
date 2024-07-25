include("../utils.jl")

function mymethod(ctx, rb, n)
    return n + 1
end

function mytopic(ctx, rb, n)
    ctx.n = n
end

mutable struct TestBag
    n::Any
    TestBag() = new(nothing)
end

function run()
    ctx = TestBag()
    srv = server(ctx)
    expose(srv, mymethod)
    subscribe(srv, mytopic)
    serve(srv, wait=false, args=Dict("ws" => 9000))
    router = caronte(wait=false, args=Dict("reset" => true))
    add_server(router, "ws://:9000/s1")

    sleep(1)
    cli = connect()
    n = 1
    response = rpc(cli, "mymethod", n)
    @test response == n + 1

    publish(cli, "mytopic", n)
    sleep(1)

    close(cli)
    shutdown()

    @test ctx.n == n
end

@info "[test_broker_server] start"
run()
@info "[test_broker_server] stop"
