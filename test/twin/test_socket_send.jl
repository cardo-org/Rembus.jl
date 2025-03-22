include("../utils.jl")

function pubsub(rb, msg)
    msg.counter = Rembus.save_message(rb.router, msg)
    Rembus.message_send(rb, msg)
end

function run()
    Rembus.request_timeout!(0.001)
    rb = connect()

    sleep(1)

    msg = Rembus.PubSubMsg(rb, "topic", "data")
    pubsub(rb, msg)
    @test rb.mark == 1
    @test msg.counter == 1

    msg = Rembus.RpcReqMsg(rb, "topic", "data")
    Rembus.message_send(rb, msg)
    sleep(0.1)

    Rembus.ack_timeout!(1e-9)
    msg = Rembus.PubSubMsg(rb, "topic", "data", Rembus.QOS2)
    pubsub(rb, msg)
    Rembus.ack_timeout!(2)
end

execute(run, "socket_send")
