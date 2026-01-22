include("../utils.jl")


myservice(val) = val;

function run()
    cli = component(anonym())
    rid = rpc(cli, "rid")
    @test rid == "test_anonym"
    close(cli)
end

execute(run, "test_anonym")
