include("../utils.jl")

function run()
    broker(ws=8010)
    rb = connect("ws://:8010/rpc_invalid_topic")

    @test_throws Rembus.RembusError rpc(rb, "")
    @test_throws Rembus.RembusError rpc(rb, "__reserved__")

    close(rb)
    shutdown()
end

@info "[test_request_invalid_topic] start"
try
    run()
catch e
    @error "[test_request_invalid_topic] error: $e"
    @test false
end
@info "[test_request_invalid_topic] end"
