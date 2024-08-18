include("ack_common.jl")

publisher = "zmq://:8002/test_ack_pub"
consumer = "zmq://:8002/test_ack_sub"
num_msg = 100

count = 0

execute(() -> run(publisher, consumer), "test_zmq_ack")

@info "[test_zmq_ack] received $count messages"
@test num_msg <= count < num_msg + 10
