include("../utils.jl")

mutable struct TestBag
    broadcast_received::Bool
end

function consume(bag, rb, data)
    @debug "[test msg_from] received: $data" _group = :test
    bag.broadcast_received = true
end

function run()
    topic = "mytopic"
    bag = TestBag(false)

    client = connect("test_retroactive_pub")
    subscriber = connect("test_retroactive_sub")
    inject(subscriber, bag)
    reactive(subscriber)
    subscribe(subscriber, topic, consume, from=LastReceived())

    close(subscriber)

    publish(client, topic, "lost in dev/null")

    subscriber = connect("test_retroactive_sub")
    inject(subscriber, bag)
    subscribe(subscriber, topic, consume, from=Now())
    reactive(subscriber)

    sleep(0.2)
    @test bag.broadcast_received === false

    close(subscriber)
    publish(client, topic, UInt8(1))
    sleep(1)

    subscriber = connect("test_retroactive_sub")
    inject(subscriber, bag)
    subscribe(subscriber, topic, consume, from=LastReceived())
    reactive(subscriber)

    #sleep(0.2)
    sleep(2)
    @test bag.broadcast_received === true

    for cli in [client, subscriber]
        close(cli)
    end
    shutdown()
end

execute(run, "test_retroactive")
