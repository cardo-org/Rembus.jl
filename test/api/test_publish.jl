using DataFrames

include("../utils.jl")

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10, y=1:10)

mutable struct TestBag
    noarg_message_received::Bool
    msg_received::Int
    df::Union{Nothing,DataFrame}
    bdf::Union{Nothing,DataFrame}
    num::Number
    TestBag() = new(false, 0, nothing, nothing)
end

function inspect(bag::TestBag)
    bag.noarg_message_received = true
end

function consume(bag::TestBag, data::Number)
    bag.msg_received += 1

    @debug "consume recv: $data" _group = :test
    bag.num = data
end

function consume(bag::TestBag, data)
    bag.msg_received += 1

    if names(data) == ["x", "y"]
        @debug "recv bdf" _group = :test
        bag.bdf = data
    else
        @debug "recv df" _group = :test
        bag.df = data
    end
end


function publish_workflow(pub, sub1, sub2, sub3, isfirst=false)
    waittime = 0.6
    my_topic = "consume"
    noarg_topic = "noarg_topic"

    testbag = TestBag()

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

    sub3 = connect(sub3)
    shared(sub3, testbag)
    subscribe(sub3, my_topic, consume, retroactive=LastReceived())
    reactive(sub3)

    #sleep(1)
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
    unsubscribe(sub1, my_topic) # by name
    unsubscribe(sub2, consume) # by function object

    # removing a not registerd interest throws an error
    try
        unsubscribe(sub1, my_topic)
        @test false
    catch e
        @debug "[remove interest]: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    if isfirst
        @info "testbag.msg_received $(testbag.msg_received) === 9"
    else
        @test testbag.msg_received === 9
        @test testbag.bdf == bdf
        @test testbag.df == df
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
    # send 4 pubsub messages
    publish_workflow("pub", "tcp://:8001/sub1", "tcp://:8001/sub2", "sub3", true)
    @info "starting real test"
    for sub1 in ["tcp://:8001/sub1", "zmq://:8002/sub1"]
        for sub2 in ["tcp://:8001/sub2", "zmq://:8002/sub2"]
            for sub3 in ["sub3", "zmq://:8002/sub3"]
                for publisher in ["pub", "zmq://:8002/pub"]
                    @debug "test_publish endpoints: $sub1, $sub2, $sub3, $publisher, " _group = :test
                    # send a grand total of 64 pubsub messages
                    publish_workflow(publisher, sub1, sub2, sub3)
                end
            end
        end
    end
end

execute(run, "test_publish")

# expect 68 messages published (received and stored by broker)
# the mark of sub1 and sub3 is 67 because they not subscribed to
# noarg_topic.
verify_counters(total=68, components=Dict("sub2" => 68, "sub1" => 67, "sub3" => 67))
