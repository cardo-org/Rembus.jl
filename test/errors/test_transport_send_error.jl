include("../utils.jl")

using HTTP
using Sockets

function Rembus.transport_send(
    twin::Rembus.Twin,
    ws::Union{WebSockets.WebSocket,TCPSocket},
    msg::Rembus.PubSubMsg{Float64}
)
    error("transport exception")
end

foo(x::Float64) = @info "foo called"

function run()
    rb = tryconnect("myc")
    sub = Rembus.connect()
    subscribe(sub, foo)
    reactive(sub)

    @test isconnected(rb)
    @test isconnected(sub)
    publish(rb, "foo", 1.0)
    sleep(1)

    close(rb)
    close(sub)
end

execute(run, "test_transport_send_error")
