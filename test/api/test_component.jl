include("../utils.jl")

mutable struct Ctx
    data::Any
end

function atopic(ctx, x)
    @info "[test_component] atopic: $x"
    ctx.data = x
end

function run()
    ctx = Ctx(nothing)

    @component "mycomponent"
    sleep(1)

    @test_throws ErrorException @subscribe atopic invalid_mode

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

    @expose aservice(ctx, x, y) = x + y

    res = rpc(rb, "aservice", [1, 2])
    @test res == 3

    @unexpose aservice
    @test_throws RpcMethodUnavailable rpc(rb, "aservice", [1, 2])

    close(rb)

    # test unknown process
    #@test_throws Visor.UnknownProcess @publish "fabulous" foo()
    try
        @publish "fabulous" foo()
    catch e
        @test isa(e, ErrorException)
        @test e.msg == "unknown process fabulous"
    finally
        @terminate
    end
end

execute(run, "test_component")
