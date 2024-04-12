include("../utils.jl")

rpc_topic = "rpc_method"
request_arg = 1
reason = "this is an error"
args_error_msg = "expected a float32"

mutable struct TestBag
    rpc_method_invoked::Bool
end

function add_one(add_one_arg)
    @atest add_one_arg == request_arg
    add_one_arg + 1
end

function do_method_error()
    error(reason)
end

function do_args_error(data)
    if !isa(data, Float32)
        throw(ErrorException(args_error_msg))
    end
end

function rpc_method(bag, rpc_method_arg)
    bag.rpc_method_invoked = true

    # expect rpc_method_arg equals to request arg
    @atest rpc_method_arg == request_arg
end

function run(request_url, subscriber_url, exposer_url)
    bag = TestBag(false)
    client = tryconnect(request_url)

    try
        rpc(client, rpc_topic, exceptionerror=true)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodNotFound)
        @test e.cid === client.client.id
        @test e.topic === rpc_topic
    end

    implementor = tryconnect(exposer_url)
    expose(implementor, rpc_topic, add_one)
    subscriber = tryconnect(subscriber_url)
    shared(subscriber, bag)
    reactive(subscriber)
    subscribe(subscriber, rpc_topic, rpc_method)

    res = rpc(client, rpc_topic, request_arg)
    @test res == 2

    try
        res = rpc(implementor, rpc_topic, exceptionerror=true)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodLoopback)
        @test e.cid === implementor.client.id
        @test e.topic === rpc_topic
    end

    expose(implementor, rpc_topic, do_method_error)
    try
        res = rpc(client, rpc_topic)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodException)
        @test e.cid === client.client.id
        @test e.topic === rpc_topic
    end

    expose(implementor, rpc_topic, do_args_error)
    try
        res = rpc(client, rpc_topic, 1.0)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodException)
        @test e.cid === client.client.id
        @test e.topic === rpc_topic
    end

    close(implementor)
    try
        res = rpc(client, rpc_topic, timeout=2)
        @test 0 == 1
    catch e
        if isa(e, Rembus.RpcMethodUnavailable)
            @test e.cid === client.client.id
            @test e.topic === rpc_topic
        else
            @warn "unexpected exception: $e"
            @test false
        end
    end

    @test bag.rpc_method_invoked === true

    implementor = tryconnect(exposer_url)
    unexpose(implementor, rpc_topic)

    for cli in [implementor, client, subscriber]
        close(cli)
    end
    sleep(0.000001)
end

Rembus.CONFIG.zmq_ping_interval = 0
Rembus.CONFIG.ws_ping_interval = 0

function run()
    for exposer_url in ["zmq://:8002/test_request_impl", "test_request_impl"]
        for subscriber_url in ["zmq://:8002/test_request_sub", "test_request_sub"]
            for request_url in ["zmq://:8002/test_request", "test_request"]
                @debug "rpc endpoints: $exposer_url, $subscriber_url, $request_url" _group = :test
                run(request_url, subscriber_url, exposer_url)
                testsummary()
            end
        end
    end
end

execute(run, "test_request_api")
