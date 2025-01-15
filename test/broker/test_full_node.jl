include("../utils.jl")

message_received = false
foo(x) = x * x

function bar(x)
    global message_received = true
end

broker_service(ctx, rb, x) = x + 1
broker_consumer(ctx, rb) = ctx["consumer"] = true

function run()
    bro = broker(log="info", ws=8000, tcp=8001, zmq=8002)
    islistening(bro, protocol=[:zmq], wait=10)

    expose(bro, broker_service)
    subscribe(bro, broker_consumer)

    for url in ["ws://:8000", "tcp://:8001", "zmq://:8002"]
        ctx = Dict()
        inject(bro, ctx)
        rb = connect(url)
        expose(rb, foo)
        subscribe(rb, bar)
        reactive(rb)

        publish(bro, "bar", 1)
        publish(rb, "broker_consumer")

        val = 2
        response = rpc(bro, "foo", val)
        @test response == val * val
        expose(bro, "latest_service", (ctx, rb, x) -> x + 2)
        response = rpc(rb, "latest_service", val)
        @test response == val + 2

        @test_throws Rembus.RpcMethodException rpc(rb, "broker_service")

        inject(bro, nothing)
        @test_throws Rembus.RpcMethodException rpc(rb, "broker_service")

        @test message_received
        close(rb)
        @test haskey(ctx, "consumer")
    end
end

@info "[test_full_node] start"
try
    run()
catch e
    @error "[test_full_node] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_full_node] stop"
