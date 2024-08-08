include("ack_common.jl")

Rembus.setup(Rembus.CONFIG)

publisher = "test_ack_pub"
consumer = "test_ack_sub"
num_msg = 100

count = 0

ts = time()
execute(() -> run(publisher, consumer), "test_ws_ack")

@info "[test_ws_ack] received $count messages"
@test num_msg <= count < num_msg + 100

# expect two messages at rest
df = Rembus.data_at_rest(string(num_msg), BROKER_NAME)
@test nrow(df) == num_msg
