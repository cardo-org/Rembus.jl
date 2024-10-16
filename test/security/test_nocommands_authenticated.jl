include("../utils.jl")

function run()
    # connect but not send any packet
    rb = Rembus.RBConnection("tcp://localhost:8001")
    Rembus._connect(rb, Rembus.NullProcess(rb.client.id))
    close(rb)
    sleep(1)
end

try
    execute(run, "test_nocommands_authenticated", mode="authenticated")
    @test true
catch e
    @error "[test_nocommands_authenticated]: $e"
    @test false
finally
end
