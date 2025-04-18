include("../utils.jl")

rpc_topic = "rpc_method"
request_arg = 1
reason = "this is an error"
args_error_msg = "expected a float32"

mutable struct TestBag
    rpc_method_invoked::Bool
end

function add_one(add_one_arg)
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

function rpc_method(bag, rb, rpc_method_arg)
    bag.rpc_method_invoked = true
end

function run(request_url, subscriber_url, exposer_url)

    connect("")

    rb = connect("tcp://:8001")
    @test isopen(rb)
    close(rb)

    bag = TestBag(false)
    client = connect(request_url)

    try
        rpc(client, rpc_topic)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodNotFound)
        @test e.topic === rpc_topic
    end

    implementor = connect(exposer_url)
    expose(implementor, rpc_topic, add_one)
    subscriber = connect(subscriber_url)
    inject(subscriber, bag)
    reactive(subscriber)
    subscribe(subscriber, rpc_topic, rpc_method)

    res = rpc(client, rpc_topic, request_arg)
    @test res == 2

    res = direct(client, "request_impl", rpc_topic, request_arg)
    @test res == 2

    try
        res = direct(client, "request_impl", "unknow_service", request_arg)
    catch e
        @test isa(e, Rembus.RpcMethodNotFound)
        @test e.topic === "unknow_service"
    end
    try
        res = direct(client, "wrong_target", rpc_topic, request_arg)
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_TARGET_NOT_FOUND
        @test e.topic === rpc_topic
    end

    try
        res = rpc(implementor, rpc_topic)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodLoopback)
        @test e.topic === rpc_topic
    end

    expose(implementor, rpc_topic, do_method_error)
    try
        res = rpc(client, rpc_topic)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodException)
        @test e.topic === rpc_topic
    end

    expose(implementor, rpc_topic, do_args_error)
    try
        res = rpc(client, rpc_topic, 1.0)
        @test 0 == 1
    catch e
        @test isa(e, Rembus.RpcMethodException)
        @test e.topic === rpc_topic
    end

    close(implementor)
    try
        res = direct(client, "request_impl", rpc_topic, request_arg)
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_TARGET_DOWN
        @test e.topic === rpc_topic
    end

    try
        res = rpc(client, rpc_topic)
        @test false
    catch e
        if isa(e, Rembus.RpcMethodUnavailable)
            @test e.topic === rpc_topic
        else
            @warn "unexpected exception: $e"
            @test false
        end
    end

    @test bag.rpc_method_invoked === true

    implementor = connect(exposer_url)
    unexpose(implementor, rpc_method)
    for rb in [implementor, client, subscriber]
        close(rb)
    end
end

function run()
    ws_ping_interval!(0)
    zmq_ping_interval!(0)
    for exposer_url in ["zmq://:8002/request_impl", "request_impl"]
        for subscriber_url in ["zmq://:8002/request_sub", "request_sub"]
            for request_url in ["zmq://:8002/request_client", "request_client"]
                @debug "rpc endpoints: $exposer_url, $subscriber_url, $request_url"
                run(request_url, subscriber_url, exposer_url)
            end
        end
    end
end

execute(run, "request")
