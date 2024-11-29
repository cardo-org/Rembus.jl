include("../utils.jl")

s1_mytopic(ctx, rb, data) = ctx["s1"] = data
s2_mytopic(ctx, rb, data) = ctx["s2"] = data

ctx = Dict()

function start_servers()
    s1 = server(ws=7000)
    inject(s1, ctx)
    subscribe(s1, "mytopic", s1_mytopic)
    s2 = server(ws=7001)
    inject(s2, ctx)
    subscribe(s2, "mytopic", s2_mytopic)
end

function policy_default()
    rb = component(["ws://:7000", "ws://:7001"])

    Rembus.whenconnected(rb) do rb
        publish(rb, "mytopic", "hello")
        sleep(0.5)
        shutdown(rb)
        @info "pubsub policy_default:$ctx"
    end
    @test length(ctx) == 2
end

function policy_all()
    rb = component(["ws://:7000", "ws://:7001"], :all)

    Rembus.whenconnected(rb) do rb
        publish(rb, "mytopic", "hello")
        sleep(0.5)
        shutdown(rb)
        @info "pubsub policy_all:$ctx"
    end
    @test length(ctx) == 2
end

function policy_round_robin()
    rb = component(["ws://:7000", "ws://:7001"], :round_robin)

    Rembus.whenconnected(rb) do rb
        publish(rb, "mytopic", "hello")
        sleep(0.5)
        @test length(ctx) == 1
        @test haskey(ctx, "s1")

        publish(rb, "mytopic", "hello")
        sleep(0.5)
        shutdown(rb)
    end
    @test length(ctx) == 2
    @test haskey(ctx, "s2")
end

function policy_first_up()
    rb = component(["ws://:7000", "ws://:7001"], :first_up)

    Rembus.whenconnected(rb) do rb
        publish(rb, "mytopic", "hello")
        sleep(0.5)
        @info "pubsub policy_firstup:$ctx"
        @test length(ctx) == 1
        @test haskey(ctx, "s1")

        publish(rb, "mytopic", "hello")
        sleep(0.5)
        shutdown(rb)
    end
    @test length(ctx) == 1
    @test haskey(ctx, "s1")
end

function test_terminate()
    rb = component(["ws://:7000", "ws://:7001"], :first_up)
    shutdown(rb)
end


@info "[test_component_publish] start"
try
    start_servers()
    for fn in [
        policy_default,
        policy_all,
        policy_first_up,
        policy_round_robin,
        test_terminate
    ]
        empty!(ctx)
        fn()
        sleep(1)
    end

catch e
    @error "[test_component_publish] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_component_publish] stop"
