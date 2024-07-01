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

function start_server()
    rb = server(Ctx(2))
    provide(rb, power)
    provide(rb, set_exponent)
    serve(rb, wait=false)
end

start_server()
