using Rembus
using Test

ts = 10000;
msgid = 1
topic = "mytopic"

mutable struct FakeSocket
    connected::Bool
    FakeSocket() = new(true)
end

sentdata(t) = Rembus.SentData(
    ts, Rembus.Msg(Rembus.TYPE_PUB, Rembus.PubSubMsg(topic), t), Timer(0))

Base.isopen(socket::FakeSocket) = socket.connected

router = Rembus.Router()

twin1 = Rembus.Twin(router, "twin1", Rembus.loopback)
twin2 = Rembus.Twin(router, "twin2", Rembus.loopback)

twin1.socket = FakeSocket()
twin2.socket = FakeSocket()

implementors = [twin1, twin2]

target = Rembus.less_busy(router, topic, implementors)
@info "target: $target"
@test target === twin1

twin1.sent[msgid] = sentdata(twin1)

target = Rembus.less_busy(router, topic, implementors)

@info "target: $target"
@test target === twin2

twin2.sent[msgid] = sentdata(twin2)

twin2.sent[msgid+1] = sentdata(twin2)

target = Rembus.less_busy(router, topic, implementors)

@info "target: $target"
@test target === twin1

twin1.socket.connected = false
twin2.socket.connected = false
target = Rembus.less_busy(router, topic, implementors)
@test target === nothing
