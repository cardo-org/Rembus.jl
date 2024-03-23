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

twin = Rembus.Twin(router, "twin", Channel())
#startup(process(Rembus.twin_task, args=(twin,)))
supervise(process(Rembus.twin_task, args=(twin,)), wait=false)

router.address2twin[identity] = twin
router.topic_impls["topic"] = Set([twin])

yield()
Rembus.destroy_twin(twin, router)

@test isempty(router.topic_impls)
@test !haskey(router.address2twin, identity)

