include("../utils.jl")


function myservice(d)
    return d
end

function run()
    component1 = tryconnect(cid1)
    anonymous = connect()

    expose(component1, myservice)

    v = rpc(component1, "version")
    @test v == Rembus.VERSION

    d = Dict(["key$i" => "value_$i" for i in 1:1000])
    res = rpc(anonymous, "myservice", d)
    @test d == res

    d = Dict(["key$i" => "value_$i" for i in 1:10000])
    res = rpc(anonymous, "myservice", d)
    @test d == res

    # simulate a wrong tcp packet
    sock = anonymous.socket
    write(sock, UInt8[1, 2, 3])
    sleep(1)
    # the wrong packet format close the connection
    @test_throws RembusTimeout rpc(anonymous, "version")

    close(component1)
end

cid1 = "component2"

ENV["REMBUS_BASE_URL"] = "tcp://127.0.0.1:8001"
execute(run, "test_tcp", args=Dict("tcp" => 8001, "reset" => true))

#ENV["REMBUS_BASE_URL"] = "tls://127.0.0.1:8001"
#execute(run, "test_tcp", args=Dict("secure" => true, "tcp" => 8001))

delete!(ENV, "REMBUS_BASE_URL")
