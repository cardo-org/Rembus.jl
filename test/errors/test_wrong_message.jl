include("../utils.jl")

foo(x) = @info "foo: $x"

function run()
    broker(ws=8010, tcp=8011)

    rb = connect("ws://:8010/wssub")
    ws = rb.socket.sock
    #  ErrorException("invalid rembus packet")
    HTTP.WebSockets.send(ws, [0x01])

    sleep(1)
    rb = connect("ws://:8010/wssub")
    ws = rb.socket.sock
    # ErrorException("unknown rembus packet type 15
    HTTP.WebSockets.send(ws, [0x81, 0xff])

    rb = connect("tcp://:8011/tcpsub")
    sock = rb.socket.sock

    # WrongTcpPacket (tcp channel invalid header value)
    write(sock, [0xab])

end

@info "[test_wrong_message] start"
try
    run()
catch e
    @error "[test_wrong_message] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_wrong_message] end"
