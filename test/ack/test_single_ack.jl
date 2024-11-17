include("../utils.jl")

function foo(ctx, rb)
    @info "[test_single_ack] recv foo"
    ctx.count += 1
end

mutable struct Ctx
    count::Int
end

function run()
    try
        ctx = Ctx(0)
        sub = connect()
        subscribe(sub, foo)
        inject(sub, ctx)
        reactive(sub)

        rb = connect()

        publish(rb, "foo", qos=QOS1)

        sleep(0.5)
        @test ctx.count == 1

        publish(rb, "foo", qos=QOS1)
        sleep(0.05)
        @test ctx.count == 2

        close(rb)
    catch e
        @error "[test_single_ack]: $e"
    end
end

execute(run, "test_single_ack")
