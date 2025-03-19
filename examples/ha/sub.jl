using Rembus

function mytopic(ctx, rb, msg)
    println("$rb: $msg")
end

function main(name)
    ctx = Dict()
    failover = get(ENV, "REMBUS_FAILOVER", "ws://:8001")
    rb = component(name, failovers=[failover])
    subscribe(rb, mytopic)
    inject(rb, ctx)
    reactive(rb)
    wait(rb)
end

main(ARGS[1])
