include("../utils.jl")


mutable struct TestContext
    prev_count::Int
    count::Int
    unordered::Int
    TestContext() = new(0, 0, 0)
end

function consume(ctx, count)
    if ctx.prev_count > count
        ctx.unordered = ctx.unordered + 1
    end
    ctx.prev_count = count
    ctx.count = ctx.count + 1
end

function send(rb, start, finish)
    for n in start:finish
        publish(rb, "consume", n)
    end
end

function run()
    ctx = TestContext()
    publisher = connect("test_park_pub")
    subscriber = connect("test_park_sub")

    subscribe(subscriber, "consume", consume)
    shared(subscriber, ctx)
    close(subscriber)

    send(publisher, 1, 10000)

    @info "reconnecting"
    subscriber = connect("test_park_sub")
    shared(subscriber, ctx)
    subscribe(subscriber, consume, true)
    reactive(subscriber)

    @async send(publisher, 10001, 20000)
    sleep(1)
    close(publisher)
    close(subscriber)

    @info "test results: count=$(ctx.count), out of orders=$(ctx.unordered)"
    @test ctx.unordered == 0
end

execute(run, "test_retroactive")
