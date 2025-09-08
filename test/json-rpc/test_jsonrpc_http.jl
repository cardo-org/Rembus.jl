include("../utils.jl")

using Base64
using DataFrames
using JSONTables

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
function jsonrpc_wrong_request(url::String, method::String, params=nothing)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => method,
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

function jsonrpc_empty_request(url::String)
    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
    )
    return JSON3.read(String(response.body), Dict)
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

function jsonrpc_connect_named(url::String, cid::String)
    auth = Base64.base64encode(cid)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "method" => "uptime",
        "id" => 999
    )

    response = HTTP.post(
        url,
        ["Authorization" => auth],
        JSON3.write(request_obj)
    )
    return JSON3.read(String(response.body), Dict)
end

function jsonrpc_invalid_type(url::String; id)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "params" => Dict(
            "__type__" => 9999
        ),
        "id" => id
    )

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    return JSON3.read(String(response.body), Dict)
end

function jsonrpc_invalid_response(url::String; id)
    request_obj = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "error" => Dict(
            "__type__" => 9999
        ),
        "id" => id
    )

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

function jsonrpc_batch(url::String, method::String, params; id)
    request_obj = [
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => method,
            "id" => id,
            "params" => params
        ),
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => "uptime",
            "id" => id + 1
        )]

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    jstr = String(response.body)
    return JSON3.read(jstr, Vector)
end

function jsonrpc_batch_mixed(url::String, method::String, params; id)
    request_obj = [
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => method,
            "id" => id,
            "params" => params
        ),
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => "foo"
        )]

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    jstr = String(response.body)
    return JSON3.read(jstr, Vector)
end

function jsonrpc_batch_notify(url::String)
    request_obj = [
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => "foo"
        ),
        Dict{String,Any}(
            "jsonrpc" => "2.0",
            "method" => "bar"
        )]

    response = HTTP.post(
        url,
        ["Content-Type" => "application/json"],
        JSON3.write(request_obj)
    )
    content = String(response.body)
    return content
end


function run()
    rembus_url = "http://localhost:9000"
    x = 10
    y = 20
    ctx = Dict()

    srv = component("jsonrpc_server")
    expose(srv, myservice)
    expose(srv, mydataframe)
    inject(srv, ctx)
    subscribe(srv, mytopic)
    reactive(srv)

    response = jsonrpc_wrong_request(
        rembus_url, "myservice", [x, y]
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

    msgid = 2
    response = jsonrpc_request(
        rembus_url, "unknow_service"; id=msgid
    )
    @test haskey(response, "error")
    @test response["id"] == msgid

    response = jsonrpc_empty_request(rembus_url)
    @test haskey(response, "error")
    @test isnothing(response["id"])

    # Try to connect with a component id already in use
    response = jsonrpc_connect_named(rembus_url, "jsonrpc_server")
    @test haskey(response, "error")
    @test isnothing(response["id"])

    msgid = 3
    jsonrpc_invalid_type(rembus_url; id=msgid)
    @test haskey(response, "error")
    @test isnothing(response["id"])

    msgid = 4
    jsonrpc_invalid_response(rembus_url; id=msgid)
    @test haskey(response, "error")
    @test isnothing(response["id"])

    msg = "hello rembus"
    response = jsonrpc_publish(
        rembus_url, "mytopic", msg
    )
    sleep(1) # wait for the topic to be processed
    @test ctx["msg"] == msg

    msgid = 5
    response = jsonrpc_request(
        rembus_url, "mydataframe"; id=msgid
    )
    @test haskey(response, "result")
    @test response["result"] == arraytable(df)
    @test response["id"] == msgid

    msgid = 6
    response = jsonrpc_request(
        rembus_url, "myservice", [x, y, "unexpected_arg"]; id=msgid
    )
    @test haskey(response, "error")
    @test response["id"] == msgid

    msgid = 7
    response = jsonrpc_batch(
        rembus_url, "myservice", [x, y]; id=msgid
    )
    @test length(response) == 2

    msgid = 8
    response = jsonrpc_batch_mixed(
        rembus_url, "myservice", [x, y]; id=msgid
    )
    @test length(response) == 1

    response = jsonrpc_batch_notify(rembus_url)
    @info "RESPONSE: $response"
    @test response == ""

end

execute(run, "test_jsonrpc_http", http=9000)
