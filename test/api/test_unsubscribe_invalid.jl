include("../utils.jl")

foo(x) = @info "foo: $x"

function run()
    broker(ws=8010)
    rb = connect("ws://:8010/unsub_invalid")

    @test_throws Rembus.RembusError unsubscribe(rb, "")
    @test_throws Rembus.RembusError unsubscribe(rb, "__reserved__")

    # idempotent
    unsubscribe(rb, "never_subscribed")
    subscribe(rb, "foo", foo)
    unsubscribe(rb, "foo")
    unsubscribe(rb, "foo")

    close(rb)
    shutdown()
end

@info "[test_unsubscribe_invalid] start"
try
    run()
catch e
    @error "[test_unsubscribe_invalid] error: $e"
    @test false
end
@info "[test_unsubscribe_invalid] end"
