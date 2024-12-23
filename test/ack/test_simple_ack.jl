include("../utils.jl")

mutable struct TestCtx
    count::UInt
end

function mytopic(ctx, rb, n)
    @info n
    ctx.count += 1
end

function run()
    ctx = TestCtx(0)

    sub = connect("subscriber")
    pub = connect("publisher")

    inject(sub, ctx)
    subscribe(sub, mytopic, from=LastReceived())
    reactive(sub)

    publish(pub, "mytopic", 1)
    publish(pub, "mytopic", 2)

    sleep(0.5)
    unreactive(sub)
    publish(pub, "mytopic", 3)

    sleep(1)
    close(pub)
    close(sub)
    @test ctx.count >= 2
end

execute(run, "test_simple_ack)")
