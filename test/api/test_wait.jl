include("../utils.jl")


myservice(val) = val;

function run()
    Timer(t -> shutdown(), 2)

    cli = component(anonym())
    rid = rpc(cli, "rid")
    @test rid == "test_wait"
    wait(cli)
end

execute(run, "test_wait")
