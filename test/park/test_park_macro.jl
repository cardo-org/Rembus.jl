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
        @publish rb consume(n)
    end
end

function subscribe(sub, ctx)
    @component sub
    @subscribe sub consume from = Now()
    @shared sub ctx
end

function run()
    ctx = TestContext()
    pub = "test_park_pub"
    sub = "test_park_sub"

    @component pub

    # just for communicating to the  broker the interest for consume topic
    subscribe(sub, ctx)
    @terminate sub

    # the subscriber was terminated, messages will be cached by the broker
    send(pub, 1, 10000)

    @info "reconnecting"
    subscribe(sub, ctx)

    @async send(pub, 10001, 20000)

    @reactive sub

    sleep(6)
    @terminate pub
    @terminate sub

    @info "test results: count=$(ctx.count), out of orders=$(ctx.unordered)"
    @test ctx.unordered == 0
end

execute(run, "test_park_macro")
