include("../utils.jl")
#
#  Initial setup:
#
#     c1_failover -- main_broker -- c2
#
#  Whenever main_broker goes down, c2 connect to c1_failover:
#
#                c1_failover -- c2
#
#  When main_broker comes back up, c1_failover reconnect to main_broker and
#  c2 reconnects to main_broker because c1_failover close its connected nodes:
#
#     c1_failover -- main_broker -- c2
#

myservice(x, y) = x + y

function run()
    Rembus.info!()

    main_broker = "main_broker"
    c1_failover = "c1_failover"

    bro = broker(ws=9000, name=main_broker)
    Rembus.islistening(bro, wait=20)

    failover_url = "ws://:9001/c2-c1"
    c1 = component("ws://:9000/$c1_failover", ws=9001)
    c2 = component("ws://:9000/c2", failovers=[failover_url])

    expose(c1, myservice)

    # Give some time to register myservice.
    sleep(0.5)

    @test rpc(c2, "rid") === main_broker
    @test rpc(c2, "myservice", 1, 2) == 3

    # Stop the main broker.
    shutdown(bro)

    # Wait for the failover to take over.
    sleep(3)

    @test rpc(c2, "rid") === c1_failover
    @test rpc(c2, "myservice", 1, 2) == 3

    # Restart the main broker.
    bro = broker(ws=9000, name=main_broker)

    # Wait for the main broker to take over.
    Rembus.islistening(bro, wait=20)

    # Reserve some time to client reconnection.
    sleep(3)

    @test rpc(c2, "rid") === main_broker
    @test rpc(c2, "myservice", 1, 2) == 3

    shutdown()
end

run()
