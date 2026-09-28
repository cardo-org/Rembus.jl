include("../utils.jl")

function mytopic(val; ctx, node)
    ctx["count"] += 1
end

function run_pub()
    # First publish n messages.
    #
    # Use QOS1 (at least once) instead of the default QOS0: QOS0 is
    # fire-and-forget and publish() only casts the message to the local
    # twin's send queue, so closing the component immediately afterwards
    # races with the actual network send/broker-side persistence. QOS1
    # makes the send block until the broker acknowledges receipt, so both
    # messages are guaranteed to be archived before the component closes.
    pub = component("upload_messages_pub")
    publish(pub, "mytopic", 1; qos=Rembus.QOS1)
    publish(pub, "mytopic", 2; qos=Rembus.QOS1)

    close(pub)
end

function run_sub1()
    ctx = Dict("count" => 0)

    # then subscribe
    sub = component("upload_messages_sub")
    inject(sub, ctx)
    subscribe(sub, mytopic, Rembus.LastReceived)
    reactive(sub)

    # Poll instead of a fixed sleep: delivery of the offline-queued messages
    # is asynchronous and its timing may vary with system load.
    max_wait = 10
    wtime = 0.1
    t = 0.0
    while t < max_wait && ctx["count"] < 2
        sleep(wtime)
        t += wtime
    end
    @test ctx["count"] == 2

    close(sub)
end

function run_sub2()
    sub = component("upload_messages_sub")
    subscribe(sub, mytopic, Rembus.LastReceived)
    reactive(sub)
    close(sub)
end

execute(run_pub, "test_upload_messages")
execute(run_sub1, "test_upload_messages", reset=false)

# find a message file with timestamp older than twin.mark
execute(run_sub2, "test_upload_messages", reset=false)
