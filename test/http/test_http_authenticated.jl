include("../utils.jl")

using Base64

Rembus.info!()

broker_name = "http_authenticated"
client = "http_authenticated_client"
server = "http_authenticated_server"
password = "mysecret"

basic_auth(str::String) = Base64.base64encode(str)

function setup()
    dirs_files = [
        (joinpath(Rembus.rembus_dir(), client), ".secret"),
        (joinpath(Rembus.rembus_dir(), server), ".secret"),
        (joinpath(Rembus.broker_dir(broker_name), "keys"), client),
        (joinpath(Rembus.broker_dir(broker_name), "keys"), server)
    ]

    for (dir, filename) in dirs_files
        mkpath(dir)
        fn = joinpath(dir, filename)
        open(fn, "w") do f
            write(f, password)
        end
    end

end

function mytopic()
    @info "[test_http_autenticated] mytopic() (NO ARGS)"
end

function mytopic(x, y)
    @info "[test_http_autenticated] mytopic($x,$y)"
end

function myservice(x, y)
    @info "[test_http_autenticated] myservice($x,$y)"
    return x + y
end

function run()
    rb = connect(server)
    expose(rb, myservice)
    subscribe(rb, mytopic)
    reactive(rb)

    x = 1
    y = 2

    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "http://localhost:9000/mytopic", [], JSON3.write([x, y])
    )

    auth = basic_auth("$client:$password")
    response = HTTP.post("http://localhost:9000/mytopic", ["Authorization" => auth])
    @info "[test_http_autenticated] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    # It is not permitted to use a name of an already connected component
    # with HTTP rest api
    auth = Base64.base64encode("$server:$password")
    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "http://localhost:9000/version",
        ["Authorization" => auth]
    )
end

# for coverage runs
ENV["REMBUS_TIMEOUT"] = 30
execute(run, broker_name, http=9000, authenticated=true, setup=setup)
delete!(ENV, "REMBUS_TIMEOUT")
