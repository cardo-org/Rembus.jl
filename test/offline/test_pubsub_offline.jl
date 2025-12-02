include("../utils.jl")

function mytopic(val; ctx, node)
    ctx["count"] += 1
end

function run()
    ctx = Dict("count" => 0)

    # First publish n messages
    pub = component("pub_offline", name="publisher")
    publish(pub, "mytopic", 1)
    publish(pub, "mytopic", 2)

    # then subscribe
    sub = component("sub_offline", name="subscriber")
    inject(sub, ctx)
    subscribe(sub, mytopic, Rembus.LastReceived)
    reactive(sub)

    sleep(0.5)
    @test ctx["count"] == 2

    close(sub)
    close(pub)
end

execute(run, "test_pubsub_offline")
