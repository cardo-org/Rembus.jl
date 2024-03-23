include("ack_common.jl")

publisher = "test_ack_pub"
consumer = "test_ack_sub"
num_msg = 100000

count = 0

execute_caronte_process(() -> run(publisher, consumer), "test_ws_ack")

@info "[test_ws_ack] received $count messages"
@test num_msg <= count < num_msg + 100