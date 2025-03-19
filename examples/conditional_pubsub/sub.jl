using Rembus

function set_indicator(ctx, rb, kpi)
    println("$rb: $kpi")
end

function main(name)
    ctx = Dict()
    rb = component(name)
    subscribe(rb, set_indicator)
    inject(rb, ctx)
    reactive(rb)
    wait(rb)
end

main(ARGS[1])
