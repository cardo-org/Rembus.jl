include("../utils.jl")

using Base64
using DataFrames

function mydataframe()
    @info "[test_http] mydataframe()"
    return DataFrame(a=1:3, b=["x", "y", "z"])
end

function mytopic()
    @info "[test_http] mytopic() (NO ARGS)"
end

function mytopic(x, y)
    @info "[test_http] mytopic($x,$y)"
end

function myservice(x, y)
    @info "[test_http] myservice($x,$y)"
    return x + y
end

function run()
    rb = connect("http_myapp")
    expose(rb, myservice)
    expose(rb, mydataframe)
    subscribe(rb, mytopic)
    reactive(rb)

    x = 1
    y = 2

    response = HTTP.post("http://localhost:9000/mytopic", [], JSON3.write([x, y]))
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.post("http://localhost:9000/mytopic")
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.post("http://localhost:9000/noarg_topic")
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.get("http://localhost:9000/myservice", [], JSON3.write([x, y]))
    @info "[test_http] GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == 3

    response = HTTP.get("http://localhost:9000/mydataframe", [])
    @info "[test_http] GET mydataframe response=$(Rembus.body(response))"
    @test typeof(Rembus.body(response)) == Vector{Any}

    response = HTTP.get("http://localhost:9000/version")
    @info "[test_http] GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == Rembus.VERSION

    response = HTTP.post("http://localhost:9000/subscribe/foo/c1")
    @test Rembus.body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/unsubscribe/foo/c1")
    @test Rembus.body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/expose/foo/c1")
    @test Rembus.body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/unexpose/foo/c1")
    @test Rembus.body(response) === nothing
    @test response.status === Int16(200)

    # It is not permitted to use a name of a connected component
    # with HTTP rest api
    auth = Base64.base64encode("http_myapp")
    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "http://localhost:9000/version",
        ["Authorization" => auth]
    )

    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "http://localhost:9000/mytopic",
        ["Authorization" => auth]
    )

end

# for coverage runs
ENV["REMBUS_TIMEOUT"] = 30
execute(run, "http", http=9000)
delete!(ENV, "REMBUS_TIMEOUT")
