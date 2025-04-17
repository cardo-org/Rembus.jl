include("../utils.jl")


myservice(val) = val;

function run()
    # Test requests timeout
    request_timeout!(0.1)
    try
        connect("timeout_component")
        #rpc(dummy, "version")
    catch e
        @info "[test_direct] expected: $e"
    end
    request_timeout!(20)

    # Starts a component just for being notified of subscribed events
    dummy = connect(Rembus.RbURL("dummy_component"), name="dummy_component")

    sleep(2)

    server = component("myserver", ws=10000)
    expose(server, myservice)

    #for url in ["ws://127.0.0.1:8000/c1", "zmq://127.0.0.1:8002/c1"]
    #for url in ["zmq://127.0.0.1:8002/c1"]
    for url in ["ws://127.0.0.1:8000/c1"]
        rb = connect(url)
        response = direct(rb, "myserver", "myservice", "hello")
        @info "response=$response"
        @test response == "hello"

        shutdown(rb)
    end

    unexpose(server, myservice)

    shutdown(dummy)
    shutdown(server)
end

execute(run, "test_direct")
