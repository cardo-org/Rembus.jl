using Rembus

Rembus.info!()

function main()
    failover = get(ENV, "REMBUS_FAILOVER", "ws://:8001")
    rb = component("myclient", failovers=[failover])

    x = 1
    y = 0.0
    for n in 1:15
        try
            result = rpc(rb, "myservice", x, y)
            @info "myservice($x, $y) = $result"
            x += 1
        catch e
            @error "myclient: $e"
        end
        sleep(2)
    end
end

main()
