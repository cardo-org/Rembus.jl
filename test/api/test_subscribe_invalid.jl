include("../utils.jl")

foo(x) = @info "foo: $x"

function run()
    broker(ws=8010)
    rb = connect("ws://:8010/sub_invalid")

    @test_throws Rembus.RembusError subscribe(rb, "foo", foo, -1)
    @test_throws Rembus.RembusError subscribe(rb, "foo", foo, NaN)
    @test_throws Rembus.RembusError subscribe(rb, "", foo)
    @test_throws Rembus.RembusError subscribe(rb, "__reserved__", foo)

    @test subscribe(rb, "foo", foo, Inf) === nothing

    close(rb)
    shutdown()
end

@info "[test_subscribe_invalid] start"
try
    run()
catch e
    @error "[test_subscribe_invalid] error: $e"
    @test false
end
@info "[test_subscribe_invalid] end"
