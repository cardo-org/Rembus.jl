include("../utils.jl")

function mymethod(ctx, rb, n)
    # covers Base.show(rb::RBServerConnection)
    @info "[test_broker_server] rb=$rb"
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
    srv = server(ctx, mode="anonymous", ws=9000)
    expose(srv, mymethod)
    subscribe(srv, mytopic)
    bro = broker(wait=false, name=BROKER_NAME)

    #islistening(wait=20, procs=["$BROKER_NAME.serve_ws"])
    islistening(bro, wait=20)

    # it seems that coverage requires some sleep time
    sleep(1)

    cli = connect()
    n = 1
    response = rpc(cli, "mymethod", n)
    @test response == n + 1

    publish(cli, "mytopic", n)
    sleep(1)

    # the server send a QOS2 message to a broker
    # this tests that Ack2 messages are delivered (and currently ignored)
    # to the broker
    publish(srv, "hello", qos=QOS2)

    close(cli)
    @test ctx.n == n
end

@info "[test_broker_server] start"
try
    init()
    run()
catch e
    @error "unexepected error: $e"
finally
    shutdown()
end
@info "[test_broker_server] stop"
