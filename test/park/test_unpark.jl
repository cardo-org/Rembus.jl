include("../utils.jl")

function Rembus.transport_send(
    twin::Rembus.Twin,
    ws::WebSockets.WebSocket,
    msg::Rembus.PubSubMsg{UInt32}
)
    global counter
    counter += 1
    #@info "[broker] sending $(msg.data)"
    pkt = [Rembus.TYPE_PUB | msg.flags, msg.topic, msg.data]
    if counter == 5
        error("I dont like packet number five")
    end
    Rembus.broker_transport_write(ws, pkt)
    return true
end

mutable struct TestContext
    count::Int
    TestContext() = new(0)
end

function consume(ctx, count)
    ctx.count = ctx.count + 1
end

function send(rb, count)
    for n in 1:count
        publish(rb, "consume", UInt32(n))
    end
end

function park_messages(count)
    publisher = connect("test_unpark_pub")
    subscriber = connect("test_unpark_sub")
    subscribe(subscriber, "consume", consume)
    close(subscriber)

    send(publisher, count)
    sleep(4)
    close(publisher)
    close(subscriber)
end

function unpark(count)
    ctx = TestContext()
    subscriber = connect("test_unpark_sub")
    subscribe(subscriber, "consume", consume, true)
    shared(subscriber, ctx)
    reactive(subscriber)

    sleep(1)
    close(subscriber)
    @info "[first round] test results: count=$(ctx.count)"
    @test ctx.count == 4

    # reopen subscriber, the error condition is not active
    subscriber = connect("test_unpark_sub")
    subscribe(subscriber, "consume", consume, true)
    shared(subscriber, ctx)
    reactive(subscriber)

    sleep(15)
    close(subscriber)

    @info "[second_round] test results: count=$(ctx.count)"
    @test ctx.count == count
end

# triggers multiple park files
counter = 0
num_messages = 100000
execute(() -> park_messages(num_messages), "test_unpark_file::park_message")
execute(() -> unpark(num_messages), "test_unpark_file::unpark_file", reset=false)
