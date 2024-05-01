include("../utils.jl")

rpc_topic = "rpc_method"
request_arg = 1

function rpc_server1(rpc_method_arg)
    return rpc_method_arg + 1
end

function rpc_server2(rpc_method_arg)
    return rpc_method_arg + 2

end

function rpc_server3(rpc_method_arg)
    return rpc_method_arg + 3
end

function run(client_url, server1_url, server2_url, server3_url)
    client = connect(client_url)

    server1 = connect(server1_url)
    expose(server1, rpc_topic, rpc_server1)

    server2 = connect(server2_url)
    expose(server2, rpc_topic, rpc_server2)

    server3 = connect(server3_url)
    expose(server3, rpc_topic, rpc_server3)

    res = rpc(client, rpc_topic, request_arg)
    @test res == 2

    res = rpc(client, rpc_topic, request_arg)
    @info "[round_robin]: result=$res"
    @test res == 3

    res = rpc(client, rpc_topic, request_arg)
    @info "[round_robin]: result=$res"
    @test res == 4

    close(server1)

    res = rpc(client, rpc_topic, request_arg)
    # server1 down, expect a response from server2
    @info "[round_robin]: result=$res"
    @test res == 3

    close(server2)
    for i in 1:2
        res = rpc(client, rpc_topic, request_arg)
        # server1 down, expect a response from server2
        @info "[round_robin]: result=$res"
        @test res == 4
    end

    close(server3)
    try
        res = rpc(client, rpc_topic, request_arg)
    catch e
        @info "[round_robin]: $e"
    end

    # no connected exposers
    @test_throws RpcMethodUnavailable rpc(client, rpc_topic, request_arg)

    server1 = connect(server1_url)
    expose(server1, rpc_topic, rpc_server1)

    res = rpc(client, rpc_topic, request_arg)
    @info "[round_robin]: result=$res"
    @test res == 2

    for cli in [client, server1]
        close(cli)
    end
    sleep(0.000001)
end

ENV["BROKER_BALANCER"] = "round_robin"

function run()
    run("rr_client", "rr_server_1", "rr_server_2", "rr_server_3")
    testsummary()
end

execute(run, "test_round_robin")
ENV["BROKER_BALANCER"] = "first_up"
