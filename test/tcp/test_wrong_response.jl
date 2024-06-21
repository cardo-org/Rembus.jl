include("../utils.jl")

function run()
    cid = "myc"
    @component "tcp://:8001/$cid"

    @rpc version()

    twin = from("$BROKER_NAME.twins.$cid")

    socket = twin.args[1].socket

    # send a wrong packet
    write(socket, UInt8[1, 2, 3])

    # the connection is closed
    @test eof(socket)

    sleep(3)
    # reconnected
    @test isopen(twin.args[1].socket)

end

execute(run, "test_wrong_response")
