include("../utils.jl")

s1_mytopic(ctx, rb, data) = ctx["s1"] = data
s2_mytopic(ctx, rb, data) = ctx["s2"] = data

ctx = Dict()

function start_servers()
    s1 = server(ws=7000)
    shared(s1, ctx)
    subscribe(s1, "mytopic", s1_mytopic)
    s2 = server(ws=7001)
    shared(s2, ctx)
    subscribe(s2, "mytopic", s2_mytopic)
end

function policy_default()
    rb = connect(["ws://:7000", "ws://:7001"])
    publish(rb, "mytopic", "hello")
    sleep(0.5)
    close(rb)
    @info "pubsub policy_default:$ctx"
    @test length(ctx) == 2
end

function policy_all()
    rb = connect(["ws://:7000", "ws://:7001"], :all)
    publish(rb, "mytopic", "hello")
    sleep(0.5)
    close(rb)
    @test length(ctx) == 2
end

function policy_round_robin()
    rb = connect(["ws://:7000", "ws://:7001"], :round_robin)
    publish(rb, "mytopic", "hello")
    sleep(0.5)
    @test length(ctx) == 1
    @test haskey(ctx, "s1")

    publish(rb, "mytopic", "hello")
    sleep(0.5)
    @test length(ctx) == 2
    @test haskey(ctx, "s2")
    close(rb)
end

function policy_first_up()
    rb = connect(["ws://:7000", "ws://:7001"], :first_up)
    publish(rb, "mytopic", "hello")
    sleep(0.5)
    @test length(ctx) == 1
    @test haskey(ctx, "s1")

    publish(rb, "mytopic", "hello")
    sleep(0.5)
    @test length(ctx) == 1
    @test haskey(ctx, "s1")
    close(rb)
end


@info "[test_publish_all] start"
try
    start_servers()
    for fn in [policy_default, policy_all, policy_first_up, policy_round_robin]
        empty!(ctx)
        fn()
        sleep(1)
    end

catch e
    @error "[test_publish_all] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_publish_all] stop"
