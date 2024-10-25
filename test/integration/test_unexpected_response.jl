include("../utils.jl")

# Send a response that the broker does not recognize

function run()
    rb = connect()
    msg = Rembus.ResMsg(UInt128(0), Rembus.STS_SUCCESS, nothing)
    Rembus.transport_send(Val(Rembus.socket), rb, msg)
    close(rb)
end

execute(run, "test_unexpected_response")
