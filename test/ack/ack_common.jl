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

    if data == 1
        @info "recv msg $data"
    end

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
    @info "sending"
    for i in 1:num_msg
        if (i % 5000) == 0
            sleep(0.01)
        end
        publish(pub, test_topic, i)
    end
    @info "done"
end

function run(publisher, consumer)
    global count
    count = 0

    ctx = TestContext()

    #sleep(2)
    pub = connect(publisher)
    sub = connect(consumer)
    shared(sub, ctx)
    enable_ack(sub)

    sleep(0.1)
    subscribe(sub, test_topic, consume, true)
    reactive(sub)

    @async storm(pub)

    sleep(0.1)

    # close and connect again
    close(sub)
    sleep(5)

    @debug "reopening $consumer" _group = :test
    sub = connect(consumer)
    enable_ack(sub)

    subscribe(sub, test_topic, consume, true)
    shared(sub, ctx)
    reactive(sub)

    @info "sleeping"
    sleep(10)
    for cli in [pub, sub]
        @info "closing $cli"
        close(cli)
    end
    #sleep(5)
    @info "end"
    @test ctx.ordered
end
