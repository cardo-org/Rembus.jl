include("../utils.jl")

using Sockets

#function run()
#    # connect but not send any packet
#    sleep(2)
#    rb = Rembus.RBConnection("tcp://localhost:8001")
#    Rembus._connect(rb, Rembus.NullProcess(rb.client.id))
#    sleep(1)
#    close(rb)
#end

function run()
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
