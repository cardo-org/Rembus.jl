using DataFrames

include("../utils.jl")

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10, y=1:10)

mutable struct TestBag
    noarg_message_received::Bool
    msg_received::Int
end

function inspect(bag::TestBag)
    bag.noarg_message_received = true
end

function consume(bag::TestBag, data::Number)
    bag.msg_received += 1

    @debug "consume recv: $data" _group = :test
    @atest data == 2 "consume(data=$data): expected data == 2"
end

function consume(bag::TestBag, data)
    bag.msg_received += 1

    @debug "df:\n$(view(data, 1:2, :))" _group = :test
    if names(data) == ["x", "y"]
        @atest size(data) == size(bdf) "data:$(size(data)) == bdf:$(size(bdf))"
    else
        @atest size(data) == size(df) "data:$(size(data)) == df:$(size(df))"
    end
end


function publish_workflow(pub, sub1, sub2, sub3, isfirst=false)
    waittime = 0.3
    my_topic = "my_topic"
    noarg_topic = "noarg_topic"

    testbag = TestBag(false, 0)

    publisher = connect(pub)

    sub1 = connect(sub1)

    shared(sub1, testbag)

    subscribe(sub1, my_topic, consume)
    reactive(sub1)

    sub2 = connect(sub2)
    shared(sub2, testbag)

    subscribe(sub2, my_topic, consume)
    subscribe(sub2, noarg_topic, inspect)
    reactive(sub2)

    # sub3 is not reactive: no messages delivered to sub3
    sub3 = connect(sub3)
    shared(sub3, testbag)
    subscribe(sub3, my_topic, consume, true)
    reactive(sub3)

    publish(publisher, my_topic, 2)
    publish(publisher, my_topic, df)
    publish(publisher, my_topic, bdf)
    # publish with no args
    try
        publish(publisher, noarg_topic)
    catch e
        @test false
    end

    sleep(waittime)
    unsubscribe(sub1, my_topic)

    # removing a not registerd interest throws an error
    try
        unsubscribe(sub1, my_topic)
        @test 0 == 1
    catch e
        @debug "[remove interest]: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    #if isfirst
    #    sleep(1)
    #else
    sleep(waittime / 4)
    #end

    if isfirst
        @info "testbag.msg_received $(testbag.msg_received) === 9"
    else
        @test testbag.msg_received === 9
    end

    for cli in [publisher, sub1, sub2, sub3]
        close(cli)
    end

    if isfirst
        @info "testbag.noarg_message_received $(testbag.noarg_message_received) === true"
    else
        @test testbag.noarg_message_received === true
    end
end

function run()
    publish_workflow("pub", "tcp://:8001/sub1", "tcp://:8001/sub2", "sub3", true)

    for sub1 in ["tcp://:8001/sub1", "zmq://:8002/sub1"]
        for sub2 in ["tcp://:8001/sub2", "zmq://:8002/sub2"]
            for sub3 in ["sub3", "zmq://:8002/sub3"]
                for publisher in ["pub", "zmq://:8002/pub"]
                    @debug "test_publish endpoints: $sub1, $sub2, $sub3, $publisher, " _group = :test
                    publish_workflow(publisher, sub1, sub2, sub3)
                    testsummary()
                end
            end
        end
    end
end

execute(run, "test_publish")
