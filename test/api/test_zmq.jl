include("../utils.jl")

using DataFrames

df = DataFrame("col" => ["a", "b"], "val" => [1, 2])
function mymethod(arg)
    @debug "[mymethod] arg: $arg" _group = :test
    df
end

function consume(arg)
    @debug "[sub]: received $arg" _group = :test
end

function run()
    @component "zmq://:8002/exposer"
    @component "zmq://:8002/subscriber"
    @component "zmq://:8002/client"

    @expose "exposer" mymethod
    sleep(0.5)

    result = @rpc "client" mymethod(DataFrame("x" => 1:3))
    @debug "rpc result = $result" _group = :test
    @test result == df

    @subscribe "subscriber" consume from = Now()
    @reactive "subscriber"

    @publish "client" consume("hello")
    @publish "client" consume(df)

    @shutdown"client"
    @shutdown "subscriber"
    @shutdown "exposer"
end

execute(run, "test_zmq")
