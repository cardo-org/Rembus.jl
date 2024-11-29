include("../utils.jl")

function run()
    rb = component(["ws://:5000", "ws://:5001"])
    # there are no connections up
    # firstup_policy logs the message:
    # no connections available: [mytopic] message not delivered
    firstup_policy(rb)

    sleep(2)

    publish(rb, "mytopic")

    shutdown(rb)
end

@info "[test_publish_online] start"
try
    run()
catch e
    @error "[test_publish_online] error: $e"
    @test false
finally
    shutdown()
end

@info "[test_publish_online] stop"
