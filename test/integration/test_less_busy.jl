include("../utils.jl")

rpc_topic = "rpc_method"
request_arg = 1

function rpc_server1(rpc_method_arg)
    sleep(1)
    return rpc_method_arg + 1
end

function rpc_server2(rpc_method_arg)
    sleep(2)
    return rpc_method_arg + 2
end

function rpc_server3(rpc_method_arg)
    sleep(3)
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

    @async begin
        res = rpc(client, rpc_topic, request_arg)
        @info "[first_call]: result=$res"
        #@test res == 3
    end
    @async begin
        res = rpc(client, rpc_topic, request_arg)
        @info "[second_call]: result=$res"
        #@test res == 3
    end
    @async begin
        res = rpc(client, rpc_topic, request_arg)
        @info "[third_call]: result=$res"
        #@test res == 3
    end
    yield()

    sleep(1.5)
    res = rpc(client, rpc_topic, request_arg)
    @info "[fourth_call]: result=$res"

    sleep(2)
    for cli in [client, server1, server2, server3]
        close(cli)
    end
end

ENV["BROKER_BALANCER"] = "less_busy"

function run()
    run("lb_client", "lb_server_1", "lb_server_2", "lb_server_3")
    testsummary()
end

execute(run, "test_less_busy")
ENV["BROKER_BALANCER"] = "first_up"
