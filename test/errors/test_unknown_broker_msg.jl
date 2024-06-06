include("../utils.jl")

function run()
    broker = from("$BROKER_NAME.broker")
    @test broker !== nothing
    put!(broker.inbox, "invalid message")
end

execute(run, "test_unknown_broker_msg")
