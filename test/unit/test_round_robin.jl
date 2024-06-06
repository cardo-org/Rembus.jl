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
twin3 = Rembus.Twin(router, "twin3", Channel())

twin1.socket = FakeSocket()
twin2.socket = FakeSocket()

implementors = [twin1, twin2, twin3]
@debug "implementors: $implementors"

target = Rembus.round_robin(router, topic, implementors)
@debug "1. target: $target"
@test target === twin1

target = Rembus.round_robin(router, topic, implementors)
@debug "2. target: $target"
@test target === twin2

target = Rembus.round_robin(router, topic, implementors)
@debug "3. target: $target"
@test target === twin1

target = Rembus.round_robin(router, topic, implementors)
@debug "4. target: $target"
@test target === twin2

twin3.socket = FakeSocket()
target = Rembus.round_robin(router, topic, implementors)
@debug "5. target: $target"
@test target === twin3

implementors = [twin2, twin3]
target = Rembus.round_robin(router, topic, implementors)
@debug "6. target: $target"
@test target === twin2

target = Rembus.round_robin(router, topic, implementors)
@debug "7. target: $target"
@test target === twin3

implementors = [twin3]
target = Rembus.round_robin(router, topic, implementors)
@debug "8. target: $target"
@test target === twin3

implementors = []
target = Rembus.round_robin(router, topic, implementors)
@debug "9. target: $target"
@test target === nothing

twin1.socket = nothing
twin2.socket = nothing
implementors = [twin1, twin2]
target = Rembus.round_robin(router, topic, implementors)
@debug "10. target: $target"
@test target === nothing
