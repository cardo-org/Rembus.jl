include("../utils.jl")

foo(x) = @info "foo: $x"

function run()
    broker = broker(ws=8010, tcp=8011, zmq=8012)

    rb = connect("ws://:8010/sub")
    results = subscribe(rb, foo)

    @test isok(results)
    @test haskey(broker.router.topic_interests, "foo")

    close(rb)
    shutdown()
end

#execute(run, "test_subscribe")
@info "[test_subscribe] start"
try
    run()
catch e
    @error "[test_subscribe] error: $e"
    @test false
end
@info "[test_subscribe] end"
