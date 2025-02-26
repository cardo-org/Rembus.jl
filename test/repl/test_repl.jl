
include("../utils.jl")

function repl_log()
    ConsoleLogger(stdout, Info, meta_formatter=Rembus.repl_metafmt) |> global_logger
end

repl_log()
@info "this is an info message"

Base.isinteractive() = true

mutable struct TestCtx
    count::UInt
end

myservice(x) = x + 1
myservice(x, y) = x + y

younger() = true

function myservice(ctx::TestCtx, rb, x)
    ctx.count += 1
    return x + 2
end

function myservice(ctx::TestCtx, rb, x, y)
    ctx.count += 1
    return x * y
end

function run()
    ctx = TestCtx(0)

    server1 = connect("repl_server1")
    expose(server1, younger)

    server2 = connect("repl_server2")
    expose(server2, myservice)


    cli = connect()
    res = rpc(cli, "myservice", 1)
    @test res == 2
    res = rpc(cli, "myservice", [1, 2])
    @test res == 3

    inject(server2, ctx)
    res = rpc(cli, "myservice", 1)
    @test res == 3
    @test ctx.count == 1

    res = rpc(cli, "myservice", [3, 2])
    @test res == 6
    @test ctx.count == 2

    try
        rpc(cli, "myservice", [3, 2, 1])
    catch e
        @info "[test_repl] expected error: $e"
    end

    try
        rpc(cli, "younger", [1])
    catch e
        @info "[test_repl] expected error: $e"
    end

    shutdown(server2)
    shutdown(cli)
end

execute(run, "repl")
