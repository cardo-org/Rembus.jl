include("../utils.jl")

using Base64
using DuckDB

broker_name = "http_admin"
admin = "http_admin_superuser"

myservice() = 100
mytopic() = nothing

function body(response::HTTP.Response)
    if isempty(response.body)
        return nothing
    else
        return JSON3.read(response.body, Any)
    end
end

basic_auth(str::String) = Base64.base64encode(str)


# set a shared secret
function init(cid, password)
    # component side
    pkfile = Rembus.pkfile(cid, create_dir=true)
    open(pkfile, create=true, write=true) do f
        write(f, password)
    end

    # broker side
    kdir = Rembus.keys_dir(broker_name)
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_base(broker_name, cid)
    open(fn, create=true, write=true) do f
        write(f, password)
    end
end


function run()
    password = "aaa"
    init(admin, password)

    # subscribe and expose some methods
    rb = connect("wss://:8000/mycomponent")
    expose(rb, myservice)
    subscribe(rb, mytopic)

    auth = basic_auth("user")
    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "https://127.0.0.1:9000/private_topic/foo", ["Authorization" => auth]
    )

    auth = basic_auth("$admin:$password")
    response = HTTP.post(
        "https://127.0.0.1:9000/private_topic/foo",
        ["Authorization" => auth]
    )
    @test response.status == 200
    @test body(response) === nothing

    response = HTTP.post(
        "https://127.0.0.1:9000/authorize/mycomponent/foo",
        ["Authorization" => auth]
    )
    @test response.status == 200
    @test body(response) === nothing

    response = HTTP.post(
        "https://127.0.0.1:9000/unauthorize/mycomponent/foo",
        ["Authorization" => auth]
    )
    @test response.status == 200
    @test body(response) === nothing

    response = HTTP.post(
        "https://127.0.0.1:9000/public_topic/foo",
        ["Authorization" => auth]
    )
    @test response.status == 200
    @test body(response) === nothing

    response = HTTP.get(
        "https://127.0.0.1:9000/admin/broker_config",
        ["Authorization" => auth]
    )
    @test response.status == 200
    @test body(response) == Dict(
        "subscribers" => Dict("mytopic" => ["mycomponent"]),
        "exposers" => Dict("myservice" => ["mycomponent"])
    )

    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "https://127.0.0.1:9000/admin/wrong_command",
        ["Authorization" => auth]
    )

    auth = basic_auth("user")
    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "https://127.0.0.1:9000/admin/broker_config",
        ["Authorization" => auth]
    )

    remove_keys(broker_name, admin)
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_https"
else
    # create keystore
    test_keystore = joinpath(tempdir(), "keystore")
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        execute(
            run,
            broker_name,
            setup=() -> set_admin(broker_name, admin),
            secure=true,
            http=9000
        )
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end
