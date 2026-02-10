include("../utils.jl")

myservice(val) = val;
cliservice() = nothing

mytopic() = nothing
clitopic() = nothing

function run()
    server = component(["pool_a", "pool_b"], ws=10000)
    expose(server, myservice)
    subscribe(server, mytopic)

    url = "ws://127.0.0.1:8000/pool_c1"
    rb = connect(url)
    expose(rb, cliservice)
    subscribe(rb, clitopic)

    response = rpc(rb, "myservice", "hello")
    @info "response=$response"
    @test response == "hello"

    response = direct(rb, "pool_a", "myservice", "hello")
    @test response == "hello"

    @test_throws RembusError direct(rb, "invalid_pool", "myservice", "hello")

    futres = Rembus.fpc(rb, "myservice", "hello")
    @test Rembus.issuccess(futres)
    @test fetch(futres) == "hello"

    sts = Rembus.fpc(rb, "unknown_method")
    @test !Rembus.issuccess(sts)
end

execute(run, "fs_pool")
