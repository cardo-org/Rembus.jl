include("../utils.jl")

subscribers = Dict(
    "mysub" => Dict("foo" => true),
)

mutable struct Ctx
    service_data::Any
    subscriber_data::Any
end

function set_subscribers()
    bdir = Rembus.broker_dir(BROKER_NAME)
    if !isdir(bdir)
        mkpath(bdir)
    end

    fn = joinpath(bdir, "subscribers.json")
    open(fn, "w") do io
        write(io, JSON3.write(subscribers))
    end

end

function foo(ctx, x)
    @info "[test_conditional_publish] foo=$x"
    ctx.service_data = x
    return "ok"
end

function foo_subscriber(ctx, val)
    @info "test_conditional_publish foo_subscriber:$val"
    ctx.subscriber_data = val
end

function run()
    try
        ctx = Ctx(nothing, nothing)
        value = "aaa"
        rb = connect()

        @component "myserver"
        @shared ctx
        @reactive
        @expose foo

        sub = connect("mysub")
        subscribe(sub, "foo", foo_subscriber, retroactive=true)
        shared(sub, ctx)
        reactive(sub)

        rpc(rb, "foo", value)

        sleep(5)
        @test ctx.service_data == value
        @test ctx.subscriber_data == value
        @terminate
        close(rb)
        close(sub)
    catch e
        @error "[test_simple_publish] error: $e"
        @test false
    end
    sleep(2)
end

execute(run, "test_conditional_publish", setup=set_subscribers)
