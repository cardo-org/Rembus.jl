include("../utils.jl")

using Dates

mutable struct Ctx
    topic_1::Int
    topic_2::Int
end

topic_1(ctx) = ctx.topic_1 += 1
topic_2(ctx) = ctx.topic_2 += 1

function run()
    ctx = Ctx(0, 0)

    rb = connect()
    publish(rb, "topic_1")
    publish(rb, "topic_2")
    sleep(1)
    publish(rb, "topic_1")
    publish(rb, "topic_2")
    close(rb)

    # Only named component may receive message from past ...
    @component "myc"
    @shared ctx
    @subscribe topic_1 from = Second(1)
    @subscribe topic_2 from = Second(2) + Microsecond(1)
    @reactive
    sleep(1)
    @terminate

    @test ctx.topic_1 == 1
    @test ctx.topic_2 == 2
end

execute(() -> run(), "test_subscribe")