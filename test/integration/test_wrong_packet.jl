include("../utils.jl")

function run()
    rb = connect()

    Rembus.transport_write(rb.socket, 1)

    sleep(1)
end

execute(run, "test_wrong_packet")
