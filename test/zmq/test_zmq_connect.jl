include("../utils.jl")

function run(rb, node_url)
    node = connect(node_url, name="component")
    Visor.dump()
    return node
end

@info "[test_zmq_connect] start"
try
    node_url = "zmq://:8012/zmq_connect_pub"
    rb = broker(zmq=8012, name="zmq_connect")

    node = connect(node_url)
    Rembus.reset_probe!(node)

    ver = rpc(node, "version")
    @test ver == Rembus.VERSION

    Rembus.zmq_ping(node)
    Rembus.probe_pprint(node)
catch e
    @error "[zmq_connect] error: $e"
    @test false
finally
    shutdown()
end
@info "[zmq_connect] end"
