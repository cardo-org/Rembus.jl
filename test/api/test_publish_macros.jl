using DataFrames

include("../utils.jl")

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10)
bdf.y = bdf.x .^ 2

function noarg(ctx)
    ctx.noarg_message_received = true
end

function mytopic(ctx, data::Number)
    ctx.msg_received += 1
    @atest data == 2 "mytopic(data=$data): expected data == 2"
end

function mytopic(ctx, data)
    ctx.msg_received += 1
    if names(data) == ["x", "y"]
        @atest size(data) == size(bdf) "data:$(size(data)) == bdf:$(size(bdf))"
    else
        @atest size(data) == size(df) "data:$(size(data)) == df:$(size(df))"
    end
end

function publish_workflow(publisher, sub1, sub2, sub3; waittime=1, testholder=missing)
    @component sub1

    @subscribe sub1 mytopic from = Now()

    if testholder !== missing
        @shared sub1 testholder
    end

    @reactive sub1

    @component sub2
    @reactive sub2

    @subscribe sub2 mytopic from = Now()
    if testholder !== missing
        @shared sub2 testholder
    end

    @subscribe sub2 noarg from = Now()

    @component sub3
    @reactive sub3

    @subscribe sub3 mytopic from = LastReceived()
    if testholder !== missing
        @shared sub3 testholder
    end

    sleep(waittime / 3)

    @component publisher
    @publish publisher mytopic(2)
    @publish publisher mytopic(df)
    @publish publisher mytopic(bdf)

    # publish with no args
    try
        @publish publisher noarg()
    catch e
        testholder !== missing && @test false
    end

    sleep(waittime)

    @unsubscribe sub1 mytopic

    #removing a not registered interest throws an error
    try
        @unsubscribe sub1 mytopic
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    sleep(waittime / 2)
    testholder !== missing && @test testholder.msg_received === 9

    @unreactive sub3

    for cli in [publisher, sub1, sub2, sub3]
        @terminate cli
    end
end

Rembus.CONFIG.zmq_ping_interval = 0

mutable struct TestHolder
    noarg_message_received::Bool
    msg_received::Int
end

function run()
    waittime = 0.8
    for sub1 in ["tcp://:8001/sub1", "zmq://:8002/sub1"]
        for sub2 in ["tcp://:8001/sub2", "zmq://:8002/sub2"]
            for sub3 in ["sub3", "zmq://:8002/sub3"]
                for publisher in ["pub", "zmq://:8002/pub"]
                    @debug "test_publish endpoints: $sub1, $sub2, $sub3, $publisher" _group = :test
                    testholder = TestHolder(false, 0)
                    publish_workflow(publisher, sub1, sub2, sub3, waittime=waittime, testholder=testholder)
                    testsummary()
                    @test testholder.noarg_message_received === true
                    waittime = 0.4
                end
            end
        end
    end
end

execute(run, "test_publish_macros")
# expect 64 messages published (received and stored by broker)
# the mark of sub1 and sub3 is 63 because they not subscribed to
# noarg_topic.
verify_counters(total=64, components=Dict("sub2" => 64, "sub1" => 64, "sub3" => 64))
