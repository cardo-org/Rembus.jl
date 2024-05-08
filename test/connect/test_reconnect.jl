include("../utils.jl")

mutable struct Ctx
    data::Any
end

function foo(ctx, x)
    @info "[test_simple_publish] foo=$x"
    ctx.data = x
end

function bar(ctx, x)
    @info "[test_reconnect] bar: $x"
end

function run()
    try
        @component "myserver"
        @subscribe foo
        @expose bar
        @reactive

        myserver = from("myserver")

        # force a process restart
        Rembus.processput!(myserver, ErrorException("boom"))
        sleep(2)
        res = @rpc version()
        @info "version: $res"
        @test isa(res, String)
    catch e
        @error "[test_reconnect] error: $e"
        @test false
    end
    @info "shutting down"
end

execute(run, "test_reconnect")
