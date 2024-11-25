include("../utils.jl")

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

function rpc_method(bag, rb, rpc_method_arg)
    bag.rpc_method_invoked = true

    # expect rpc_method_arg equals to request arg
    @atest rpc_method_arg == request_arg
end

function run()
    request_url = "zmq://:8002/test_request"
    exposer_url = "zmq://:8002/test_request_impl"

    bag = TestBag(false)
    client = connect(request_url)

    implementor = tryconnect(exposer_url)
    expose(implementor, "rpc_method", add_one)

    res = direct(client, "test_request_impl", "rpc_method", request_arg)
    @test res == 2

    try
        res = direct(client, "test_request_impl", "unknow_service", request_arg)
    catch e
        @info "expected exception: $e"
        @test isa(e, Rembus.RpcMethodNotFound)
        @test e.cid === "test_request"
        @test e.topic === "unknow_service"
    end

    try
        res = direct(client, "wrong_target", "rpc_method", request_arg)
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_TARGET_NOT_FOUND
        @test e.cid === "test_request"
        @test e.topic === "rpc_method"
    end

    close(implementor)

    try
        res = direct(client, "test_request_impl", "rpc_method", request_arg)
    catch e
        @info "expected exception: $e"
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_TARGET_DOWN
        @test e.cid === "test_request"
        @test e.topic === "rpc_method"
    end

    for cli in [implementor, client]
        close(cli)
    end
end

Rembus.CONFIG.zmq_ping_interval = 0
Rembus.CONFIG.ws_ping_interval = 0

execute(run, "test_direct")
