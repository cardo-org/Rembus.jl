include("../utils.jl")


function run()
    # force a connection
    @rpc version()

    # the first supervised process is caronte_test
    proc = collect(values(from(".").processes))[2]

    Rembus.processput!(proc, ErrorException("boom"))
    sleep(1)
    res = @rpc version()
    @test isa(res, String)
    sleep(1)
end

execute(run, "test_rembus_task")
