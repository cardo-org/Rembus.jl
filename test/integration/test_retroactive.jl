include("../utils.jl")

mutable struct TestBag
    broadcast_received::Bool
end

function consume(bag, data)
    @debug "[test retroactive] received: $data" _group = :test
    bag.broadcast_received = true
end

function run()
    topic = "mytopic"
    bag = TestBag(false)

    client = connect("test_retroactive_pub")
    subscriber = connect("test_retroactive_sub")
    shared(subscriber, bag)
    reactive(subscriber)
    subscribe(subscriber, topic, consume, retroactive=true)

    close(subscriber)

    publish(client, topic, "lost in dev/null")

    subscriber = connect("test_retroactive_sub")
    shared(subscriber, bag)
    subscribe(subscriber, topic, consume, retroactive=false)
    reactive(subscriber)

    sleep(0.2)
    @test bag.broadcast_received === false

    close(subscriber)
    publish(client, topic, UInt8(1))
    sleep(1)

    subscriber = connect("test_retroactive_sub")
    shared(subscriber, bag)
    subscribe(subscriber, topic, consume, retroactive=true)
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
