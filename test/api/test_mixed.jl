include("../utils.jl")

using DataFrames

mutable struct TestHolder
    request_arg::String
    TestHolder() = new("")
end

df = DataFrame("col" => ["a", "b"], "val" => [1, 2])

function mymethod(arg)
    @debug "[exposer] arg: $arg" _group = :test
    df
end

function mymethod(bag, arg)
    @debug "[subscriber]: received $arg" _group = :test
    bag.request_arg = arg
end

function run()
    @debug "starting ..." _group = :test
    bag = TestHolder()

    @component "exposer"
    @component "zmq://:8002/client"
    @component "zmq://:8002/subscriber"

    @expose "exposer" mymethod

    @subscribe "subscriber" mymethod
    @shared "subscriber" bag
    @enable_ack "subscriber"
    @reactive "subscriber"

    invalue = "pippo"
    result = @rpc "client" mymethod(invalue)
    @debug "rpc result = $result" _group = :test
    @test result == df
    @test bag.request_arg == invalue

    @unexpose "exposer" mymethod
    @disable_ack "subscriber"

    @terminate "client"
    @terminate "subscriber"
    @terminate "exposer"

end

execute(run, "test_mixed", args=Dict("ws" => 8000, "zmq" => 8002, "debug" => true))
delete!(ENV, "REMBUS_DEBUG")
