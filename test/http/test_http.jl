include("../utils.jl")

# tests: 10

function mytopic(x, y)
    @info "[test_http] mytopic($x,$y)"
end

function myservice(x, y)
    @info "[test_http] myservice($x,$y)"
    return x + y
end

function run()
    @component "myapp"

    @expose myservice
    @subscribe mytopic
    @reactive

    x = 1
    y = 2

    response = HTTP.post("http://localhost:9000/mytopic", [], JSON3.write([x, y]))
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.post("http://localhost:9000/noarg_topic")
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.get("http://localhost:9000/myservice", [], JSON3.write([x, y]))
    @info "[test_http] GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == 3

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

    @terminate
end

# for coverage runs
ENV["REMBUS_TIMEOUT"] = 20
execute(run, "test_http")
delete!(ENV, "REMBUS_TIMEOUT")
