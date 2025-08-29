include("../utils.jl")

using Base64

function myservice_noctx(; x, y)
    return x + y
end

function myservice(ctx, rb; x, y)
    return x + y
end

function mytopic(ctx, rb, msg)
    ctx["msg"] = msg
end

function jsonrpc_request(url::String, method::String, params=nothing; id)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => method,
        "id" => id
    )
    if params !== nothing
        request_obj["params"] = params
    end

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    return JSON3.read(String(response.body), Dict)
end

function jsonrpc_publish(url::String, method::String, params=nothing)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => method
    )
    if params !== nothing
        request_obj["params"] = params
    end

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    return nothing
end


function run()
    rembus_url = "http://localhost:9000"
    x = 10
    y = 20
    ctx = Dict()

    srv = component("jsonrpc_server")
    expose(srv, myservice)
    inject(srv, ctx)
    subscribe(srv, mytopic)
    reactive(srv)

    srv_noctx = component("jsonrpc_server_no_context")
    expose(srv_noctx, myservice_noctx)

    msgid = 1
    response = jsonrpc_request(
        rembus_url, "myservice", Dict("x" => x, "y" => y); id=msgid
    )
    @test haskey(response, "result")
    @test response["result"] == x + y
    @test response["id"] == msgid

    msgid = 2
    response = jsonrpc_request(
        rembus_url, "myservice_noctx", Dict("x" => x, "y" => y); id=msgid
    )
    @test haskey(response, "result")
    @test response["result"] == x + y
    @test response["id"] == msgid

    msg = "hello rembus"
    response = jsonrpc_publish(
        rembus_url, "mytopic", msg
    )
    sleep(1) # wait for the topic to be processed
    @test ctx["msg"] == msg

    close(srv)
    close(srv_noctx)
end

execute(run, "test_jsonrpc_errors", http=9000)
