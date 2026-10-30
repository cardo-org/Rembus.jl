include("../utils.jl")

loopback_topic = "loopback_method"
loopback_method(x) = x + 1

function run()
    implementor = connect("loopback_impl")
    client = connect("loopback_client")
    expose(implementor, loopback_topic, loopback_method)
    @test rpc(client, loopback_topic, 1) == 2

    # the implementor cannot call its own method
    @test_throws Rembus.RpcMethodLoopback rpc(implementor, loopback_topic, 1)

    # the failed loopback must not remove the implementor from the router
    @test rpc(client, loopback_topic, 1) == 2
end

execute(run, "loopback_exposure")
