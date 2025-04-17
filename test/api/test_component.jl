include("../utils.jl")

myservice(val) = val;

function run()
    # Test requests timeout
    Rembus.error!()
    request_timeout!(0)
    try
        connect("rpc_timeout_component")
    catch e
        @info "[test_rpc] expected: $e"
    end
    request_timeout!(20)

    Rembus.debug!()
    # Starts a component just for being notified of subscribed events
    dummy = connect(Rembus.RbURL("rpc_dummy_component"), name="rpc_dummy_component")

    Rembus.warn!()

    server = component("rpc_myserver", ws=10000)
    expose(server, myservice)

    Rembus.info!()
    router = from("component.broker").args[1]
    for node in router.network
        @info "router node: $node"
    end

    #for url in ["ws://127.0.0.1:8000/c1", "zmq://127.0.0.1:8002/c1"]
    #for url in ["zmq://127.0.0.1:8002/c1"]
    for url in ["ws://127.0.0.1:8000/rpc_c1"]
        rb = connect(url)
        response = rpc(rb, "myservice", "hello")
        @info "response=$response"
        @test response == "hello"

        response = direct(rb, "rpc_myserver", "myservice", "hello")
        @test response == "hello"

        futres = Rembus.fpc(rb, "myservice", ("hello",))
        @test Rembus.issuccess(futres)
        @test fetch(futres) == "hello"

        sts = Rembus.fpc(rb, "unknown_method")
        @test !Rembus.issuccess(sts)

        shutdown(rb)
    end

    unexpose(server, myservice)

    shutdown(dummy)
    shutdown(server)
    Rembus.info!()
end

execute(run, "component")
