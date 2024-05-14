using Distributed
addprocs(2)

@everywhere using Rembus
using Test

mutable struct Ctx
    data::Any
end

function foo(ctx, x)
    @info "[test_simple_publish] foo=$x"
    ctx.data = x
end

function start_broker()
    remotecall(caronte, 2, wait=false, exit_when_done=false)
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
    remotecall(shutdown, 2)
    sleep(2)
end

@async start_broker()
sleep(15)
run()
