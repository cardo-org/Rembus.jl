include("../utils.jl")


function run()
    empty_cid()
    send_attestation()
    already_identified()
    wrong_admin_command()
end

function send_attestation()
    rb = connect()
    msg = Rembus.Attestation(rb, "cid", UInt8[0x1, 0x2, 0x3], Dict())
    res_future = Rembus.send_msg(rb, msg)
    @test_throws RembusError fetch(res_future)
end

function already_identified()
    # Identity and Attestation on an identified connection are rejected.
    rb = connect("already_identified_node")
    msg = Rembus.IdentityMsg(rb, "another_node")
    try
        fetch(Rembus.send_msg(rb, msg))
        @test false
    catch e
        @test e.reason == "already identified"
    end

    msg = Rembus.Attestation(rb, "another_node", UInt8[0x1], Dict())
    try
        fetch(Rembus.send_msg(rb, msg))
        @test false
    catch e
        @test e.reason == "already identified"
    end
    @test isopen(rb)
    close(rb)
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

execute(run, "unexpected_messages", ws=8338)
