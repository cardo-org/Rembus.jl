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
    rb = server()
    inject(rb, Ctx(2))
    expose(rb, power)
    expose(rb, set_exponent)
    wait(rb)
end

start_server()
