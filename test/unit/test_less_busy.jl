using Rembus
using Test

ts = 10000;
msgid = 1
topic = "mytopic"

struct FakeSocket
end

sentdata(t) = Rembus.SentData(
    ts, Rembus.Msg(Rembus.TYPE_PUB, Rembus.PubSubMsg(topic), t))

Base.isopen(socket::FakeSocket) = true

router = Rembus.Router()

twin1 = Rembus.Twin(router, "twin1", Channel())
twin2 = Rembus.Twin(router, "twin2", Channel())

twin1.sock = FakeSocket()
twin2.sock = FakeSocket()

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