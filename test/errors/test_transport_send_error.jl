include("../utils.jl")

# tests: 3

using HTTP
using Sockets

function Rembus.transport_send(
    ::Val{Rembus.socket},
    twin::Rembus.Twin,
    msg::Rembus.PubSubMsg
)
    @info "generate a transport exception broker side: $(typeof(msg))"
    error("transport exception")
end

called = false
function foo(x::Float64)
    global called
    @info "foo called: $x"
    called = true
end

function run()
    rb = tryconnect("tcp://:9000/send_error_component")
    sub = Rembus.connect("tcp://:9000")
    subscribe(sub, foo)
    reactive(sub)

    @test isconnected(rb)
    @test isconnected(sub)
    publish(rb, "foo", 1.0)
    sleep(1)
    close(rb)
    close(sub)
    @test !called
end

execute(
    run, "test_transport_send_error", tcp=9000, ws=nothing, zmq=nothing, islistening=["test_tcp"]
)
