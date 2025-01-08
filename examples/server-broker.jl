using DataFrames
using Rembus

mutable struct Ctx
    exponent::Float64
end

function power(ctx, component, df::DataFrame)
    df.y = df.x .^ ctx.exponent
    return df
end

function power(ctx, component, n::Number)
    return n^ctx.exponent
end

set_exponent(ctx, component, value) = ctx.exponent = value

function start_exposer_server()
    rb = server(Ctx(2), ws=7000)
    expose(rb, power)
    expose(rb, set_exponent)
    return rb
end

function start_requestor()
    rb = server(ws=7001)
    return rb
end

function start_broker()
    router = broker(wait=false)
    add_node(router, "ws://127.0.0.1:7000/exposer")
    add_node(router, "ws://127.0.0.1:7001/client")
    return router
end

srv1 = start_exposer_server()
srv2 = start_requestor()

brok = start_broker()

whenconnected(srv2) do rb
    result = rpc(rb, "power", 2)
    @info "result=$result"
end

shutdown()
