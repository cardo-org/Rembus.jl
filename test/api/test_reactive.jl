include("../utils.jl")

using Dates

mutable struct Ctx
    topic_1::Int
end

topic_1(ctx, rb) = ctx.topic_1 += 1

function publish_msg()
    rb = connect()
    publish(rb, "topic_1")
    sleep(1)
    close(rb)
end

function run()
    ctx = Ctx(0)

    myc = connect("myc")
    inject(myc, ctx)
    subscribe(myc, topic_1, from=Second(1))
    reactive(myc, from=Now())
    sleep(1)
    close(myc)

    # Only named component may receive message from past ...
    @component "myc"
    @inject ctx
    @subscribe topic_1 from = Second(1)

    # 1 microsec just for cover the skip file check (Rembus.start_reactive)
    @reactive from = Microsecond(1)
    sleep(1)
    @shutdown

    @test ctx.topic_1 == 0
end

execute(() -> publish_msg(), "test_reactive::1")
execute(() -> run(), "test_reactive::2", reset=false)
