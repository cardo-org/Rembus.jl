include("../utils.jl")

function myservice1(x)
    sleep(1)
    x + 1
end

myservice2(x) = x * x

function start_server(port, fn)
    emb = server(ws=port)
    expose(emb, "myservice", fn)
end

function run()
    server_1 = start_server(9000, myservice1)
    server_2 = start_server(9001, myservice2)
    Rembus.islistening(wait=20, procs=["server.serve:9000", "server.serve:9001"])

    Visor.dump()
    cli = connect(
        ["ws://localhost:9000/cmp1", "ws://localhost:9001/cmp2"], :round_robin
    )

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 6

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 25

    cli.policy = :first_up

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 6

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 6

    @async begin
        res = rpc(cli, "myservice", 5)
        @info "[first_up] res=$res"
        @test res == 6
    end

    yield()
    cli.policy = :less_busy

    res = rpc(cli, "myservice", 5)
    @info "[less_busy] result=$res"
    @test res == 25

    sleep(2)
    close(cli)

    # test connection errors
    cli = connect(["ws://localhost:9998/cmp1", "ws://localhost:9999/cmp2"])
    @info "[test_rbpool] done"
    shutdown()
end

run()
