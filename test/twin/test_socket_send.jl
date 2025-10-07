include("../utils.jl")


mark = Rembus.uts()

function pubsub(rb, msg)
    push!(rb.router.archiver.inbox, msg)

    msg.counter = mark

    Rembus.message_send(rb, msg)
end

function run()
    request_timeout!(0.001)
    rb = connect()

    sleep(1)

    msg = Rembus.PubSubMsg(rb, "topic", "data")
    pubsub(rb, msg)
    @test rb.mark == mark
    @test msg.counter == mark

    msg = Rembus.RpcReqMsg(rb, "topic", "data")
    Rembus.message_send(rb, msg)
    sleep(0.1)

    ack_timeout!(1e-9)
    msg = Rembus.PubSubMsg(rb, "topic", "data", Rembus.QOS2)
    pubsub(rb, msg)
    ack_timeout!(2)
end

execute(run, "socket_send")
