include("../utils.jl")

# Send a response that the broker does not recognize

function run()
    rb = connect()
    msg = Rembus.AdminReqMsg(Rembus.BROKER_CONFIG, Dict("nocmd" => nothing))
    @info "sending"
    Rembus.transport_send(rb.socket, msg)
    @info "done"
    close(rb)
    @info "closed"
end

execute(run, "test_wrong_admin_command")
