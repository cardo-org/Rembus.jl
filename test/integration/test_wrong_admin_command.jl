include("../utils.jl")

# Send a response that the broker does not recognize

function run()
    rb = connect()
    msg = Rembus.AdminReqMsg(Rembus.BROKER_CONFIG, Dict("nocmd" => nothing))
    @info "sending"
    Rembus.transport_send(Val(Rembus.socket), rb, msg)
    @info "done"
    sleep(0.1)
    close(rb)
    @info "closed"
end

execute(run, "test_wrong_admin_command")
