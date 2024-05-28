include("../utils.jl")
using ZMQ

function send_wrong_packet()
    broker = from("caronte.broker")
    router = broker.args[1]
    socket = router.zmqsocket
    identity = router.twin2address["cut"]

    send(socket, identity, more=true)
    send(socket, Message(), more=true)
    # send a wrong message id 0x17
    send(socket, encode([0x17, "topic"]), more=true)
    send(socket, "data", more=true)
    send(socket, Rembus.MESSAGE_END, more=false)
end

function run()
    Rembus.CONFIG.stacktrace = true

    # wrong protocol
    @test_throws ErrorException Rembus.connect("foo://bar")

    rb = tryconnect("myc")

    # a wrong response get discarded and produce an error log
    Rembus.parse_msg(rb, "expecting a cbor packet")

    res = rpc(rb, "version")
    @test isa(res, String)
    close(rb)

    # a ping to a closed socket logs an error
    Rembus.ping(rb.socket)

    @test !Rembus.isconnected(rb)

    rb = Rembus.connect("zmq://:8002/cut")
    sleep(0.2)

    # just logs an error
    send_wrong_packet()
    sleep(0.2)

    # triggers a warning log when closing the rembus handler
    close(rb.context)
    close(rb)

    # wrong zmq endpoint does not trigger error
    # because of ZeroMQ connection logic
    rb = Rembus.connect("zmq://:8003")
    close(rb)

    @test_throws Base.IOError Rembus.connect("tcp://:8004")

    rb = Rembus.connect("nosecret")

    # an error resending the attestate just logs an error
    Rembus.resend_attestate(rb, Rembus.ResMsg(UInt128(0), Rembus.STS_SUCCESS, nothing))
    sleep(0.1)

    close(rb)

    ENV["REMBUS_BASE_URL"] = "foo://127.0.0.1:8000"
    try
        rb = Rembus.connect("myc")
        @test false
    catch e
        @test true
        @info "[test_rembus_errors] wrong REMBUS_BASE_URL: $e"
    finally
        delete!(ENV, "REMBUS_BASE_URL")
    end
end

execute(run, "test_rembus_errors")
