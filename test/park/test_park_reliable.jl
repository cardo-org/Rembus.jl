include("../utils.jl")

lck = ReentrantLock()

mutable struct TestContext
    prev_count::Int
    count::Int
    unordered::Int
    TestContext() = new(0, 0, 0)
end

function consume(ctx, count)
    lock(lck) do
        if ctx.prev_count > count
            ctx.unordered = ctx.unordered + 1
        end
        ctx.count = ctx.count + 1
        ctx.prev_count = count
        #if count % 1000 == 0 || count > 19999
        @info "recv $count ($(ctx.count))"
        #end
    end
end

function send(rb, start, finish)
    for n in start:finish
        publish(rb, "consume", n)
    end
end

function set_subscriber(ctx)
    subscriber = connect("test_park_sub")
    shared(subscriber, ctx)
    subscribe(subscriber, consume, true)
    return subscriber
end

function run()
    ctx = TestContext()
    publisher = connect("test_park_pub")
    subscriber = connect("zmq://:8002/test_park_sub")

    subscribe(subscriber, "consume", consume)
    shared(subscriber, ctx)
    close(subscriber)

    send(publisher, 1, 10000)

    @info "reconnecting"
    subscriber = set_subscriber(ctx)
    reactive(subscriber)

    @async send(publisher, 10001, 20000)

    sleep(1)
    close(subscriber)

    subscriber = set_subscriber(ctx)
    reactive(subscriber)
    sleep(10)
    #
    close(publisher)
    close(subscriber)

    @info "test results: count=$(ctx.count), out of orders=$(ctx.unordered)"
    @test ctx.unordered == 0
    @test ctx.count == 20000
end

execute(run, "test_park")
