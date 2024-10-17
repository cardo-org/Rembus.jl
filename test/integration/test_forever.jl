include("../utils.jl")

function consume(data)
    @debug "[test_forever_sub] received: $data"
end

function test_forever()
    topic = "mytopic"
    subscriber = connect("test_forever_sub")
    subscribe(subscriber, topic, consume, from=LastReceived())
    forever(subscriber)
end

function run()

    # call keep_alive
    Rembus.CONFIG.ws_ping_interval = 1

    @test Rembus.islistening(
        wait=30,
        procs=[
            "$(BROKER_NAME).serve_ws",
            "$(BROKER_NAME).serve_tcp",
            "$(BROKER_NAME).serve_zeromq"
        ]
    )

    @async test_forever()
    sleep(5)
    shutdown()
end

execute(run, "test_forever")
