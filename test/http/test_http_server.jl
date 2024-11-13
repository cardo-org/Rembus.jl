include("../utils.jl")

using Base64

# tests: 11

basic_auth(str::String) = Base64.base64encode(str)

function mytopic(x, y)
    @info "[test_http] mytopic($x,$y)"
end

function myservice(x, y)
    @info "[test_http] myservice($x,$y)"
    return x + y
end

# set a shared secret
function init(cid, password)
    # component side
    pkfile = Rembus.pkfile(cid)
    open(pkfile, create=true, write=true) do f
        write(f, password)
    end

    # server side
    kdir = Rembus.keys_dir(SERVER_NAME)
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_base(SERVER_NAME, cid)
    open(fn, create=true, write=true) do f
        write(f, password)
    end
end

function start_server()
    try
        rb = server(http=8080, name=SERVER_NAME, log="info")
        expose(rb, myservice)
        subscribe(rb, mytopic)
        @info "$rb setup completed"
        forever(rb)
    catch e
        @error "[test_http_server]: $e"
    end
end

function run(user, password)
    x = 1
    y = 2

    is_up = Rembus.islistening(procs=["server_test.serve:8000"], wait=20)
    Visor.dump()

    response = HTTP.post("http://localhost:8080/mytopic", [], JSON3.write([x, y]))
    @info "[test_http] POST response=$(Rembus.body(response))"
    @test Rembus.body(response) === nothing

    response = HTTP.get("http://localhost:8080/myservice", [], JSON3.write([x, y]))
    @info "[test_http] GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == 3

    response = HTTP.get("http://localhost:8080/version")
    @info "[test_http] GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == Rembus.VERSION

    # force an authenticated connection
    authenticated!()

    # with HTTP rest api
    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "http://localhost:8080/version"
    )

    auth = basic_auth("$user:$password")
    response = HTTP.get(
        "http://localhost:8080/version",
        ["Authorization" => auth]
    )
    @info "[test_http] auth GET response=$(Rembus.body(response))"
    @test Rembus.body(response) == Rembus.VERSION

    auth = basic_auth("$user:wrong_password")
    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "http://localhost:8080/mytopic",
        ["Authorization" => auth],
        JSON3.write([x, y])
    )

    anonymous!()
end

# for coverage runs
ENV["REMBUS_TIMEOUT"] = 30
@info "[test_http_server]: start"
user = "user"
password = "pwd"

try
    init(user, password)
    @async start_server()
    run(user, password)
catch e
    @test false
finally
    shutdown()
    remove_keys(user)
end
@info "[test_http_server]: stop"
delete!(ENV, "REMBUS_TIMEOUT")
