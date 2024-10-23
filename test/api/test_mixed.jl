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

    @subscribe "subscriber" mymethod from = Now()
    @shared "subscriber" bag
    @reactive "subscriber"

    invalue = "pippo"
    result = @rpc "client" mymethod(invalue)
    @debug "rpc result = $result" _group = :test
    @test result == df
    @test bag.request_arg == invalue

    @unexpose "exposer" mymethod

    @terminate "client"
    @terminate "subscriber"
    @terminate "exposer"

end

ENV["REMBUS_DEBUG"] = "1"
execute(run, "test_mixed", ws=8000, zmq=8002, log="debug")
delete!(ENV, "REMBUS_DEBUG")
