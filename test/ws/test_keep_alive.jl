include("../utils.jl")

function run()
    ws_ping_interval!(0.1)
    node = connect("keep_alive_node")
    shutdown(node)
    sleep(0.5)
end

execute(run, "keep_alive", ws=8000)
