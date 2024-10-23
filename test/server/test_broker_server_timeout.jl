include("../utils.jl")

#=
Test a IdentityMsg response timeout
=#

function run()
    server(ws=9000)
    router = caronte(wait=false, reset=true, name=BROKER_NAME)
    add_server(router, "ws://:9000/s1")

    sleep(1)
    twin = from("$BROKER_NAME.twins.s1").args[1]

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

    shutdown()
end

@info "[test_broker_server_timeout] start"
try
    run()
catch e
    @error "[test_broker_server_timeout]: unexpected: $e"
end
@info "[test_broker_server_timeout] stop"
