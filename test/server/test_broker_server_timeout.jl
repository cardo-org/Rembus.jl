include("../utils.jl")

#=
Test a IdentityMsg response timeout
=#

function run()
    server(ws=9000)
    router = broker(wait=false, reset=true, name=BROKER_NAME)
    add_node(router, "ws://:9000/s1")

    sleep(1)
    twins = from("$BROKER_NAME.twins")
    twin = from_name(twins, "ws://127.0.0.1:9000/s1").args[1]
    msg = Rembus.IdentityMsg(twin.router.process.supervisor.id)
    try
        Rembus.wait_response(
            twin,
            Rembus.Msg(Rembus.TYPE_IDENTITY, msg, twin),
            0 # timeout
        )
    catch e
        @info "[test_broker_server_timeout] expected timeout: $e"
        @test isa(e, RembusTimeout)
    end
end

@info "[test_broker_server_timeout] start"
try
    run()
catch e
    @error "[test_broker_server_timeout]: unexpected: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
end
@info "[test_broker_server_timeout] stop"
