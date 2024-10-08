include("../utils.jl")

function consume(data)
    @debug "[test_forever_sub] received: $data" _group = :test
end

function run()

    # call keep_alive
    Rembus.CONFIG.ws_ping_interval = 1

    topic = "mytopic"
    @test Rembus.islistening(
        wait=30,
        procs=[
            "$(BROKER_NAME).serve_ws",
            "$(BROKER_NAME).serve_tcp",
            "$(BROKER_NAME).serve_zeromq"
        ]
    )

    Timer(tmr -> shutdown(), 5)

    subscriber = connect("test_forever_sub")
    subscribe(subscriber, topic, consume, from=LastReceived())

    forever(subscriber)
    @info "[test_forever] done" _group = :test
end

execute(run, "test_forever")
