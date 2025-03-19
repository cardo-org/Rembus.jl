using Rembus

function myservice(x, y)
    return x + y
end

function main()
    failover = get(ENV, "REMBUS_FAILOVER", "ws://:8001")
    url = Rembus.RbURL(failover)
    rb = component("srv1", ws=url.port)
    expose(rb, myservice1)
    wait(rb)
end

main()
