using Rembus
using Test

function task(pd, router)
    router.process = pd
    for msg in pd.inbox
        @isshutdown(msg)
    end
end

identity = UInt8[0, 1, 2, 3, 4]
router = Rembus.Router()
proc = process("router", task, args=(router,))

twin = Rembus.Twin(router, "twin")
supervise([
        supervisor("broker", [
            proc,
            process(Rembus.twin_task, args=(twin,))
        ])
    ], wait=false)

@test !isauthenticated(twin)

router.address2twin[identity] = twin
router.topic_impls["topic"] = Set([twin])

rembusMessage = Rembus.PubSubMsg("mytopic", "mydata")
msg = Rembus.Msg(Rembus.TYPE_PUB, rembusMessage, twin)
@info "build msg: $msg"
msg = Rembus.Msg(Rembus.TYPE_PUB, Rembus.PubSubMsg("mytopic", zeros(UInt8, 11)), twin)
@info "build msg: $msg"

@test_throws ErrorException Rembus.transport_send(twin, nothing, msg)

# encode a wrong Rembus message type value
@test_throws ErrorException Rembus.broker_parse(encode([0x17, "topic"]))

# park an anonymous twin does nothing
Rembus.park(nothing, twin, rembusMessage)

twin.id = "mytwin"
twin.hasname = true
twin.pager = Rembus.Pager(IOBuffer(; write=true, read=true))
twin_dir = joinpath(Rembus.broker_dir("broker"), "twins")
mkpath(joinpath(twin_dir, twin.id))

Rembus.park(nothing, twin, rembusMessage)
yield()
Rembus.destroy_twin(twin, router)

@test !isempty(router.topic_impls)
@test !haskey(router.address2twin, identity)
