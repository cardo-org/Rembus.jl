include("../utils.jl")

function myservice1(session, x)
    sleep(1)
    x + 1
end

myservice2(session, x) = x * x

function start_server(port, fn)
    emb = embedded()
    provide(emb, "myservice", fn)
    serve(emb, port, wait=false, exit_when_done=false)
end

function run()
    server_1 = start_server(9000, myservice1)
    server_2 = start_server(9001, myservice2)
    Rembus.islistening(20, procs=["embedded.serve:9000", "embedded.serve:9001"])
    #sleep(10)

    Visor.dump()
    cli = connect(["ws://localhost:9000/cmp1", "ws://localhost:9001/cmp2"])
    set_balancer("round_robin")

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 6

    res = rpc(cli, "myservice", 5)
    @info "result=$res"
    @test res == 25

    set_balancer("first_up")

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
    set_balancer("less_busy")

    res = rpc(cli, "myservice", 5)
    @info "[less_busy] result=$res"
    @test res == 25

    sleep(2)
    close(cli)
    shutdown()
end

run()
