include("../utils.jl")

function mytopic(val; ctx, node)
    ctx["count"] += 1
end

function run_pub()
    # First publish n messages
    pub = component("upload_messages_pub")
    publish(pub, "mytopic", 1)
    publish(pub, "mytopic", 2)

    close(pub)
end

function run_sub1()
    ctx = Dict("count" => 0)

    # then subscribe
    sub = component("upload_messages_sub")
    inject(sub, ctx)
    subscribe(sub, mytopic, Rembus.LastReceived)
    reactive(sub)
    sleep(2)
    @test ctx["count"] == 2

    close(sub)
end

function run_sub2()
    sub = component("upload_messages_sub")
    subscribe(sub, mytopic, Rembus.LastReceived)
    reactive(sub)
    close(sub)
end

execute(run_pub, "test_upload_messages")
execute(run_sub1, "test_upload_messages", reset=false)

# find a message file with timestamp older than twin.mark
execute(run_sub2, "test_upload_messages", reset=false)
