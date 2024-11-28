using Rembus
using Test

function task(pd, router)
    router.process = pd
    for msg in pd.inbox
        @isshutdown(msg)
    end
end

identity = UInt8[0, 1, 2, 3, 4]
router = Rembus.Router("broker")
proc = process("router", task, args=(router,))

topic_1 = "topic_1"
topic_2 = "topic_2"

twin1 = Rembus.Twin(router, "twin1", Rembus.loopback)
twin2 = Rembus.Twin(router, "twin2", Rembus.loopback)
twin3 = Rembus.Twin(router, "twin3", Rembus.loopback)

router.topic_interests[topic_1] = Set([twin1, twin2])
router.topic_interests[topic_2] = Set([twin2, twin3])

# find twin2's topics
@test topic_1 in Rembus.twin_topics(twin2)
@test topic_2 in Rembus.twin_topics(twin2)

@test Rembus.twin_topics(twin1) == [topic_1]
@test Rembus.twin_topics(twin3) == [topic_2]
