include("../utils.jl")

test_topic = "acktopic"
current = 0

mutable struct TestContext
    ordered::Bool
    TestContext() = new(true)
end

function consume(ctx, data)
    global current
    global count
    global ts

    #@info "[ack_common::consume] recv msg $data"
    if current > data
        ctx.ordered = false
        delta = time() - ts
        @error "out of order, current:$current, data:$data (delta: $delta)"
    end
    current = data

    count += 1
    if (count % 1000) == 0
        delta = time() - ts
        @info "$count records received in $delta secs"
    end
end

function storm(pub)
    global ts
    ts = time()
    @info "[storm] sending $num_msg messages"
    for i in 1:num_msg
        if (i % 5000) == 0
            sleep(0.01)
        end
        publish(pub, test_topic, i, qos=QOS_1)
    end
    @info "[storm] done"
end

function run(publisher, consumer)
    global count
    count = 0

    ctx = TestContext()

    #sleep(2)
    pub = connect(publisher)
    sub = connect(consumer)
    shared(sub, ctx)

    sleep(0.1)

    subscribe(sub, test_topic, consume, true)
    reactive(sub)

    @async storm(pub)

    sleep(0.1)

    # close and connect again
    close(sub)
    sleep(2)

    @debug "reopening $consumer" _group = :test
    sub = connect(consumer)

    subscribe(sub, test_topic, consume, true)
    shared(sub, ctx)
    reactive(sub)

    @info "sleeping"
    sleep(5)
    for cli in [pub, sub]
        @info "closing $cli"
        close(cli)
    end
    @info "end"
    @test ctx.ordered
end
