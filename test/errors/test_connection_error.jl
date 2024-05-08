include("../utils.jl")
using HTTP


function run()
    rb = tryconnect("myc")
    @test isconnected(rb)
    close(rb.socket, WebSockets.CloseFrameBody(1008, "Unexpected client websocket error"))
    sleep(1)
    @test !isconnected(rb)

    rb = connect("tcp://:8001")
    @test isa(rpc(rb, "version"), String)
    close(rb)

end

execute(run, "test_connection_error")
