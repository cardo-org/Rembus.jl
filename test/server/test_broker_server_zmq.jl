include("../utils.jl")

function mymethod(ctx, rb, n)
    # covers Base.show(rb::RBServerConnection)
    @info "[test_broker_server_zmq] rb=$rb"
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
        @info "[test_broker_server_zmq]: making dir $d"
        mkdir(d)
    end

    fn = joinpath(d, "servers.json")
    open(fn, "w") do io
        write(io, JSON3.write(["zmq://:9002/s1"]))
    end
end

function run()
    ctx = TestBag()
    srv = server(ctx, mode="anonymous", zmq=9002)
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
    sleep(1)
    close(cli)
    @test ctx.n == n
end

@info "[test_broker_server_zmq] start"
try
    init()
    run()
catch e
    @test false
    @error "unexpected error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[test_broker_server_zmq] stop"
