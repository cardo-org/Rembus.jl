include("../utils.jl")

mutable struct Ctx
    data::Any
end

struct WrongMsg
end

function atopic(ctx, rb, x)
    @info "[test_rpc_req] atopic: $x"
    ctx.data = x
end

function myservice()
    @info "[test_rpc_req] myservice requested"
    return 1
end

function run()
    bro = broker(wait=false)
    islistening(bro, protocol=[:ws])
    rb = connect("foo")

    # a wrong request logs an error, it is a rembus_task async call
    Rembus.blocking_request(rb, WrongMsg())
end

@info "[test_rpc_req] start"

try
    run()
catch e
    @error "[test_rpc_req] unexpected error: $e"
    @test false
finally
    shutdown()
end
@info "[test_rpc_req] stop"
