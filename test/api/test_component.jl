include("../utils.jl")

mutable struct Ctx
    data::Any
end

function atopic(ctx, x)
    @info "[test_component] atopic: $x"
    ctx.data = x
end

function aservice(ctx, x, y)
    return x + y
end

function run()
    ctx = Ctx(nothing)

    @component "mycomponent"
    sleep(1)
    @subscribe atopic
    @shared ctx
    @reactive

    value = 1.0
    rb = connect("pub")
    publish(rb, "atopic", value)

    sleep(0.5)
    @test ctx.data == value

    ctx.data = nothing
    @unreactive
    publish(rb, "atopic", value)
    sleep(0.5)
    @test ctx.data === nothing

    @unsubscribe atopic

    @expose aservice

    res = rpc(rb, "aservice", [1, 2])
    @test res == 3

    @unexpose aservice
    @test_throws RpcMethodUnavailable rpc(rb, "aservice", [1, 2])

    close(rb)
end

execute(run, "test_component")
