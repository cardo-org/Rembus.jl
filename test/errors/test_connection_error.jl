include("../utils.jl")
using HTTP


function run()
    rb = tryconnect("myc")
    @test isconnected(rb)
    close(rb.socket, WebSockets.CloseFrameBody(1008, "Unexpected client websocket error"))
    sleep(1)
    @test !isconnected(rb)

    rb = connect("tcp://:8001/freddy")
    @test isa(rpc(rb, "version"), String)

    @test_throws Rembus.AlreadyConnected connect("freddy")

    close(rb)

    rb = connect()
    msg = Rembus.IdentityMsg("")
    response = Rembus.wait_response(rb, msg, 2)
    @test response.status == Rembus.STS_GENERIC_ERROR

end

execute(run, "test_connection_error")
