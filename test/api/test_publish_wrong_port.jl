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


function publish_workflow(pub, sub)
    waittime = 1
    my_topic = "my_topic"
    noarg_topic = "noarg_topic"

    testbag = TestBag(false, 0)

    publisher = connect(pub)

    @info "connecting"
    sub = connect(sub)
    @info "connecting done"

    shared(sub, testbag)

    subscribe(sub, my_topic, consume)
    reactive(sub)

    @info "publish msg"
    publish(publisher, my_topic, 2)
    @info "done msg"

    sleep(waittime)

    @test testbag.msg_received === 1

    for cli in [publisher, sub]
        close(cli)
    end

    #@test testbag.noarg_message_received === true
end

function run()
    publish_workflow("pub", "ws://:8001/sub1")

    #    for sub1 in ["tcp://:8001/sub1", "zmq://:8002/sub1"]
    #        for sub2 in ["tcp://:8001/sub2", "zmq://:8002/sub2"]
    #            for sub3 in ["sub3", "zmq://:8002/sub3"]
    #                for publisher in ["pub", "zmq://:8002/pub"]
    #                    @debug "test_publish endpoints: $sub1, $sub2, $sub3, $publisher, " _group = :test
    #                    publish_workflow(publisher, sub1, sub2, sub3)
    #                    testsummary()
    #                end
    #            end
    #        end
    #    end
end

execute(run, "test_publish_ack")
