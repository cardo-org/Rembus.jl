include("../utils.jl")

function mymethod(ctx, rb, n)
    # covers Base.show(rb::RBServerConnection)
    @info "[test_server_multiple_broker] mymethod($n)"
    return n + 1
end

function mytopic(ctx, rb, n)
    @info "[test_server_multiple_broker] mytopic: received $n"
    ctx.n = n
end

mutable struct TestBag
    n::Any
    TestBag() = new(nothing)
end

function init(broker_name)
    d = Rembus.broker_dir(broker_name)
    if !isdir(d)
        @info "[test_server_multiple_broker]: making dir $d"
        mkdir(d)
    end

    fn = joinpath(d, "servers.json")
    open(fn, "w") do io
        write(io, JSON3.write(["ws://:9000/$broker_name"]))
    end
end

function run()
    ctx = TestBag()
    srv = server(mode="anonymous", ws=9000)

    for (broker_name, broker_port) in [("broker_1", 8000), ("broker_2", 8001)]
        init(broker_name)
        broker(wait=false, name=broker_name, ws=broker_port)
        Rembus.islistening(wait=20, procs=["$broker_name.serve_ws"])
    end

    # it seems that coverage requires some sleep time
    sleep(1)

    rb = connect()
    expose(rb, mymethod)
    subscribe(rb, mytopic)
    reactive(rb)
    inject(rb, ctx)

    n = 1
    response = rpc(srv, "mymethod", n)
    @test response == n + 1

    lessbusy_policy(srv)
    publish(srv, "mytopic", n)
    sleep(1)

    close(rb)
    @test ctx.n == n
end

@info "[test_server_multiple_broker] start"
try
    run()
catch e
    @error "unexepected error: $e"
    @test false
finally
    shutdown()
end
@info "[test_server_multiple_broker] stop"
