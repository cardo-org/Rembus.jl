include("../utils.jl")

function run()
    broker = from("caronte.broker")
    put!(broker.inbox, "invalid message")
end

execute(run, "test_unknown_broker_msg")
