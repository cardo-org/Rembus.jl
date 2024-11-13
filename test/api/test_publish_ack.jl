using DataFrames

include("../utils.jl")

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10, y=1:10)

mutable struct TestBag
    noarg_message_received::Bool
    msg_received::Int
end

function inspect(bag::TestBag, rb)
    bag.noarg_message_received = true
end

function consume(bag::TestBag, rb, data::Number)
    bag.msg_received += 1

    @debug "consume recv: $data" _group = :test
    @atest data == 2 "consume(data=$data): expected data == 2"
end

function consume(bag::TestBag, rb, data)
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

    sub = connect(sub)

    shared(sub, testbag)

    subscribe(sub, my_topic, consume)
    reactive(sub)

    publish(publisher, my_topic, 2, qos=QOS1)

    sleep(waittime)

    @test testbag.msg_received === 1
    for cli in [publisher, sub]
        close(cli)
    end

end

function run()
    publish_workflow("pub", "ws://:8000/sub1")
end

execute(run, "test_publish_ack")
