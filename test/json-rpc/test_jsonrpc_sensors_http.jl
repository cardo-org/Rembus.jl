include("../utils.jl")

using Base64
using DataFrames
using JSONTables

node_name = "aaa/bbb/sensor-1"

df = DataFrame(a=1:3, b=["x", "y", "z"])

function myservice(ctx, rb, x, y)
    return x + y
end

function mydataframe(ctx, rb)
    return df
end

function mytopic(ctx, rb, msg)
    ctx["msg"] = msg
end

"""
Send a wrong JSON-RPC request.

# Arguments
- `url::String`: the endpoint of the JSON-RPC server
- `method::String`: the RPC method name
- `params`: the parameters for the method (can be Array or Dict)
- `id`: the request id (default = 1)

# Returns
- Parsed JSON response
"""
function jsonrpc_wrong_request(url::String, params=nothing)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => "$node_name/wrong_method",
        "id" => 999
    )
    request_obj["params"] = "unexpected_string_instead_of_array"

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    return JSON3.read(String(response.body), Dict)
end


function jsonrpc_request(url::String, method::String, params=nothing; id)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => "$node_name/$method",
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

function jsonrpc_connect_named(url::String, cid::String)
    auth = Base64.base64encode(cid)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => "$node_name/uptime",
        "id" => 999
    )

    response = HTTP.post(
        url,
        ["Authorization" => auth],
        JSON3.write(request_obj)
    )
    return JSON3.read(String(response.body), Dict)
end


function jsonrpc_publish(url::String, method::String, params=nothing)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => "$node_name/$method"
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

    srv = component(node_name)
    expose(srv, myservice)
    expose(srv, mydataframe)
    inject(srv, ctx)
    subscribe(srv, "$node_name/mytopic", mytopic)
    reactive(srv)

    response = jsonrpc_wrong_request(
        rembus_url, [x, y]
    )
    @test haskey(response, "error")
    @test isnothing(response["id"])

    msgid = 1
    response = jsonrpc_request(
        rembus_url, "myservice", [x, y]; id=msgid
    )
    @test haskey(response, "result")
    @test response["result"] == x + y
    @test response["id"] == msgid

    # Try to connect with a component id already in use
    response = jsonrpc_connect_named(rembus_url, node_name)
    @test haskey(response, "error")
    @test isnothing(response["id"])

    msg = "hello rembus"
    response = jsonrpc_publish(
        rembus_url, "mytopic", msg
    )
    sleep(1) # wait for the topic to be processed
    @test ctx["msg"] == msg

    msgid = 2
    response = jsonrpc_request(
        rembus_url, "mydataframe"; id=msgid
    )
    @test haskey(response, "result")
    @test response["result"] == arraytable(df)
    @test response["id"] == msgid

    msgid = 3
    response = jsonrpc_request(
        rembus_url, "myservice", [x, y, "unexpected_arg"]; id=msgid
    )
    @test haskey(response, "error")
    @test response["id"] == msgid

end

execute(run, "test_jsonrpc_http", http=9000)
