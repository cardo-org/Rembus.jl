include("../utils.jl")

using ZMQ

struct InvalidMsg <: Rembus.RembusMsg
    id::UInt128
    InvalidMsg() = new(Rembus.id())
end

struct PartialMsg <: Rembus.RembusMsg
    id::UInt128
    PartialMsg() = new(Rembus.id())
end

function Rembus.transport_send(
    ::Val{Rembus.zdealer}, rb::Rembus.RBConnection, msg::InvalidMsg
)
    send(rb.socket, Message(), more=true)
    send(rb.socket, encode([104]), more=true)
    send(rb.socket, "aaaa", more=true)
    send(rb.socket, Rembus.MESSAGE_END, more=false)
    return true
end

function Rembus.transport_send(
    ::Val{Rembus.zdealer}, rb::Rembus.RBConnection, msg::PartialMsg
)
    send(rb.socket, Message(), more=true)
    send(rb.socket, encode([Rembus.TYPE_PUB, "mytopic"]), more=true)
    send(rb.socket, encode("my_value"), more=false)
    return true
end

function mymethod(arg)
    @debug "[mymethod] arg: $arg" _group = :test
    1
end

function consume(arg)
    @debug "[sub]: received $arg" _group = :test
end

function run()
    rb = Rembus.connect("zmq://:8002/test_zmq_error")

    try
        @debug "sending a corrupted packet" _group = :test
        Rembus.rpcreq(rb, InvalidMsg(), exceptionerror=true, timeout=2)
        @test false
    catch e
        @debug "expected error: $(e.msg)" _group = :test
        @test isa(e, Rembus.RembusTimeout)
    end
    @test isopen(rb.socket)
    version = Rembus.rpc(rb, "version")
    @test version === Rembus.VERSION

    Rembus.rembus_write(rb, PartialMsg())

    version = Rembus.rpc(rb, "version")
    @test version === Rembus.VERSION

    close(rb)
end

execute(run, "test_zmq_error")
