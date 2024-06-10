include("../utils.jl")

MSG_COUNT = 4

function run()

    pub = connect("publisher")
    pub_twin = from("$BROKER_NAME.twins.publisher")

    for i in 1:MSG_COUNT
        Rembus.publish_ack(pub, "my_topic", [i])
        sleep(0.1)
        if i == 3
            schedule(pub_twin.task, ErrorException("fatal"), error=true)
        end
    end
    sleep(Rembus.ACK_WAIT_TIME + 1)
    close(pub)
end

execute(run, "test_publish_side")
