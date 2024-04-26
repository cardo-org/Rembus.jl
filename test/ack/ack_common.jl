include("../utils.jl")

test_topic = "acktopic"

function consume(data)
    global count
    global ts

    count += 1
    #if (count % 10000) == 0
    if (count % 10000) == 0
        delta = time() - ts
        @info "$count records received in $delta secs"
    end
    #print(".")
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

    #sleep(2)
    pub = connect(publisher)
    sub = connect(consumer)
    enable_ack(sub)

    sleep(0.1)
    reactive(sub)

    subscribe(sub, test_topic, consume, true)

    @async storm(pub)

    sleep(2)

    # close and connect again
    close(sub)
    sleep(2)

    @debug "reopening $consumer" _group = :test
    sub = connect(consumer)
    enable_ack(sub)
    #sub = connect("zmq://10.220.18.228:8002/test_ack_sub")

    subscribe(sub, test_topic, consume, true)
    reactive(sub)

    @info "sleeping"
    sleep(15)
    for cli in [pub, sub]
        @info "closing $cli"
        close(cli)
    end
    #sleep(5)
    @info "end"
end

## # for jit
## comp = connect("compile_component")
## close(comp)
