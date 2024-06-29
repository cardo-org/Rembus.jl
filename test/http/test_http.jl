include("../utils.jl")

function mytopic(x, y)
    @info "[test_http] mytopic($x,$y)"
end

function myservice(x, y)
    @info "[test_http] myservice($x,$y)"
    return x + y
end

function body(response::HTTP.Response)
    if isempty(response.body)
        return nothing
    else
        return JSON3.read(response.body, Any)
    end
end

function run()
    @component "myapp"

    @expose myservice
    @subscribe mytopic
    @reactive

    x = 1
    y = 2

    response = HTTP.post("http://localhost:9000/mytopic", [], JSON3.write([x, y]))
    @info "[test_http] POST response=$(body(response))"
    @test body(response) === nothing

    response = HTTP.get("http://localhost:9000/myservice", [], JSON3.write([x, y]))
    @info "[test_http] GET response=$(body(response))"
    @test body(response) == 3

    response = HTTP.post("http://localhost:9000/subscribe/foo/c1")
    @test body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/unsubscribe/foo/c1")
    @test body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/expose/foo/c1")
    @test body(response) === nothing
    @test response.status === Int16(200)

    response = HTTP.post("http://localhost:9000/unexpose/foo/c1")
    @test body(response) === nothing
    @test response.status === Int16(200)

    @terminate
end

execute(run, "test_http")
