include("../utils.jl")

using Preferences

function testcase(puburl)
    Rembus.ack_timeout!(1e-20)
    pub = connect(puburl)
    Rembus.info!()
    publish(pub, "topic", (1, 2, 3), qos=Rembus.QOS1)
    close(pub)
    @info "$puburl closed"
    Rembus.ack_timeout!(2)
end

function run()
    for pub in ["ack_timeout_pub", "zmq://:8002/ack_timeout_pub"]
        testcase(pub)
    end
end

execute(run, "ack_timeout")
