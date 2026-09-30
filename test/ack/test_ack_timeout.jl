include("../utils.jl")

using Preferences

# Pretend to send the ack but never actually deliver it, so publish() always
# times out waiting for it. Without this, the broker's real ack races against
# the (deliberately tiny) ack_timeout: on a loaded machine, or with the very
# low latency zmq transport, the real ack can occasionally win the race and
# arrive before the timer fires, making the @test_throws below flaky.
function Rembus.transport_send(socket::Rembus.AbstractPlainSocket, msg::Rembus.AckMsg)
    return true
end

function Rembus.transport_send(z::Rembus.ZRouter, msg::Rembus.AckMsg)
    return true
end

function testcase(puburl)
    ack_timeout!(1e-20)
    pub = connect(puburl)
    Rembus.info!()
    # With an unreasonably small ack_timeout, publish() now blocks until the
    # ack wait gives up and throws because the ack could not be received.
    @test_throws ErrorException publish(pub, "topic", (1, 2, 3), qos=Rembus.QOS1)
    close(pub)
    @info "$puburl closed"
    ack_timeout!(2)
end

function run()
    for pub in ["ack_timeout_pub", "zmq://:8336/ack_timeout_pub"]
        testcase(pub)
    end
end

execute(run, "ack_timeout")
