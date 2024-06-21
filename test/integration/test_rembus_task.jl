include("../utils.jl")


function run()
    # force a connection
    @rpc version()

    proc = from("rembus")

    Rembus.processput!(proc, ErrorException("boom"))
    sleep(1)
    res = @rpc version()
    @test isa(res, String)
    sleep(1)
end

execute(run, "test_rembus_task")
