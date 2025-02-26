include("../utils.jl")


function run()
    empty_cid()
    send_attestation()
    wrong_admin_command()
end

function send_attestation()
    rb = connect()
    msg = Rembus.Attestation("cid", UInt8[0x1, 0x2, 0x3], Dict())
    res_future = Rembus.send_msg(rb, msg)
    @test_throws RembusError fetch(res_future)
end

function empty_cid()
    # Send an empty cid.
    try
        rb = connect()
        msg = Rembus.IdentityMsg(rb, "")
        res_future = Rembus.send_msg(rb, msg)
        fetch(res_future)
        @test false
    catch e
        @test e.reason === "empty cid"
    end
end

function wrong_admin_command()
    rb = connect()
    msg = Rembus.AdminReqMsg(rb, "topic", Dict())
    res_future = Rembus.send_msg(rb, msg)
    @test_throws RembusError fetch(res_future)
end

execute(run, "unexpected_messages", ws=8000)
