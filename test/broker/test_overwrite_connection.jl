include("../utils.jl")

using DataFrames
using Dates

broker_name = "overwrite_connection"
rounds = 10

mutable struct Ctx
    count::Int
    Ctx() = new(0)
end

function foo(ctx, rb, x)
    @info "foo recv: $x"
    ctx.count += 1
end

function wait_message(fn, max_wait=10)
    wtime = 0.1
    t = 0
    while t < max_wait
        t += wtime
        sleep(wtime)
        if fn()
            return true
        end
    end
    return false
end

function run()
    name = "overwrite_connection_node"
    bro = broker(name=broker_name, ws=8000)
    bro.router.settings.overwrite_connection = true
    Rembus.islistening(bro, protocol=[:ws], wait=10)

    rb1 = connect(name)
    #sleep(1)
    rb2 = component(name, name="another_supervisor")
    sleep(1)
    @test !isopen(rb1)
    @test isopen(rb2)

end


@info "[overwrite_connection] start"
try
    run()
catch e
    @error "[overwrite_connection] error: $e"
    @test false
finally
    shutdown()
end
@info "[overwrite_connection] end"
