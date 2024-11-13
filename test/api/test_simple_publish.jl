include("../utils.jl")

mutable struct Ctx
    data::Any
end

function foo(ctx, rb, x)
    @info "[test_simple_publish] foo=$x"
    ctx.data = x
end

function run()
    try
        ctx = Ctx(nothing)
        value = "hello"
        sleep(2)
        rb = connect()
        shared(rb, ctx)
        subscribe(rb, foo)
        reactive(rb)

        @component "mypub"
        @publish foo(value)

        sleep(8)
        @test ctx.data == value
        @terminate
    catch e
        @error "[test_simple_publish] error: $e"
        @test false
    end
    @info "shutting down"
end

execute(run, "test_simple_publish")
