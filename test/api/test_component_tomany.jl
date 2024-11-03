include("../utils.jl")

function run()
    server(ws=8000)
    server(ws=8001)

    Rembus.islistening(wait=20, procs=["server.serve:8000", "server.serve:8001"])

    #c = component(["ws://:8000", "ws://:8001", "ws://:8003"])
    c = component(["ws://:8000", "ws://:8001"])

    while (!isconnected(c))
        sleep(0.1)
    end
    sleep(1)
    result = rpc(c, "version")
    @info "[test_component_tomany] result=$result"

    shutdown()
end

run()
