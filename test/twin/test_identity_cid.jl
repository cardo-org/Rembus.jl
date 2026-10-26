include("../utils.jl")

# The Identity cid sent to the peer is the bare component id, not the URL.
function run()
    captured = Channel{Any}(1)
    server = HTTP.WebSockets.listen!("127.0.0.1", 8943) do ws
        data = HTTP.WebSockets.receive(ws)
        pkt = decode(data)
        put!(captured, pkt)
        HTTP.WebSockets.send(
            ws,
            encode([Rembus.TYPE_RESPONSE, pkt[2], Rembus.STS_SUCCESS, nothing])
        )
        sleep(0.5)
        close(ws)
    end

    cli = component("ws://127.0.0.1:8943/test_identity_cid")
    pkt = take!(captured)
    @test pkt[1] == Rembus.TYPE_IDENTITY
    @test pkt[3] == "test_identity_cid"
    close(cli)
    close(server)
end

execute(run, "test_identity_cid")
