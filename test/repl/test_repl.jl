
include("../utils.jl")

# NOTE: This test does not trace logs becaus in REPL mode logging is disabled
Rembus.repl_log()
@info "this is an info log"

Base.isinteractive() = true

mutable struct TestCtx
    count::UInt
end

myservice(x) = x + 1
myservice(x, y) = x + y

function myservice(ctx::TestCtx, x)
    ctx.count += 1
    return x + 2
end

function myservice(ctx::TestCtx, x, y)
    ctx.count += 1
    return x * y
end

function run()
    ctx = TestCtx(0)

    sub = tryconnect("repl_sub")
    expose(sub, myservice)

    cli = connect()
    res = rpc(cli, "myservice", 1)
    @test res == 2
    res = rpc(cli, "myservice", [1, 2])
    @test res == 3

    shared(sub, ctx)
    res = rpc(cli, "myservice", 1)
    @test res == 3
    @test ctx.count == 1

    res = rpc(cli, "myservice", [3, 2])
    @test res == 6
    @test ctx.count == 2

    close(sub)
    close(cli)
end

execute(run, "test_repl")
