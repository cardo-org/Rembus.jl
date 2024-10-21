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

function init()
    d = Rembus.broker_dir(BROKER_NAME)
    if !isdir(d)
        @info "[test_broker_server]: making dir $d"
        mkdir(d)
    end

    fn = joinpath(d, "servers.json")
    open(fn, "w") do io
        write(io, JSON3.write(["ws://:9000/s1"]))
    end
end

function run()
    ctx = TestBag()
    srv = server(ctx, mode="anonymous", args=Dict("ws" => 9000))
    expose(srv, mymethod)
    subscribe(srv, mytopic)
    caronte(wait=false, args=Dict("name" => BROKER_NAME))

    Rembus.islistening(wait=20, procs=["$BROKER_NAME.serve_ws"])

    # it seems that coverage require some sleep time
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
init()
run()
@info "[test_broker_server] stop"
