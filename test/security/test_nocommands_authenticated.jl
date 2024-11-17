include("../utils.jl")

using Sockets

function run()
    Rembus.islistening(
        wait=10,
        procs=["$(BROKER_NAME).serve_ws"]
    )

    sock = Sockets.connect("localhost", 8001)
    close(sock)
end

try
    execute(
        run,
        "test_nocommands_authenticated",
        mode="authenticated",
        islistening=["serve_tcp"]
    )
    @test true
catch e
    @error "[test_nocommands_authenticated]: $e"
    @test false
finally
end
