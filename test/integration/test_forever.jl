include("../utils.jl")

function consume(data)
    @debug "[test_forever_sub] received: $data" _group = :test
end

function run()
    topic = "mytopic"
    @test Rembus.islistening(30)

    Timer(tmr -> shutdown(), 5)

    subscriber = connect("test_forever_sub")
    subscribe(subscriber, topic, consume, true)

    forever(subscriber)
    @info "[test_forever] done" _group = :test
end

execute(run, "test_forever")
