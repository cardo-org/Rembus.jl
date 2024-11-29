include("../utils.jl")

using Base64

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

basic_auth(str::String) = Base64.base64encode(str)

# set a shared secret
function init(cid, password)
    # component side
    pkfile = Rembus.pkfile(cid)
    open(pkfile, create=true, write=true) do f
        write(f, password)
    end

    # broker side
    kdir = Rembus.keys_dir(BROKER_NAME)
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_base(BROKER_NAME, cid)
    open(fn, create=true, write=true) do f
        write(f, password)
    end
end

function run()
    authenticated_component = "bar"
    password = "aaa"
    init(authenticated_component, password)

    @component "wss://:8000/myapp"

    @expose myservice
    @subscribe mytopic
    @reactive

    x = 1
    y = 2

    auth = basic_auth("mycomponent")
    response = HTTP.post(
        "https://127.0.0.1:9000/mytopic", ["Authorization" => auth], JSON3.write([x, y])
    )
    @info "[test_http] POST response=$(body(response))"
    @test response.status == 200
    @test body(response) === nothing

    # send a password for a component not registered
    auth = basic_auth("mycomponent:mysecret")
    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "https://127.0.0.1:9000/mytopic", ["Authorization" => auth], JSON3.write([x, y])
    )

    # send the right password
    auth = basic_auth("$authenticated_component:$password")
    response = HTTP.post(
        "https://127.0.0.1:9000/mytopic", ["Authorization" => auth], JSON3.write([x, y])
    )
    @test response.status == 200
    @test body(response) === nothing

    response = HTTP.get(
        "https://127.0.0.1:9000/myservice", ["Authorization" => auth], JSON3.write([x, y])
    )
    @test response.status == 200
    @test body(response) == x + y

    response = HTTP.get(
        "https://127.0.0.1:9000/myservice", ["Authorization" => auth], JSON3.write([x, y])
    )
    @test response.status == 200
    @test body(response) == x + y

    # send an unknown service
    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "https://127.0.0.1:9000/unknown", ["Authorization" => auth], JSON3.write([x, y])
    )

    # send the wrong password
    auth = basic_auth("$authenticated_component:wrong_pwd")
    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "https://127.0.0.1:9000/mytopic", ["Authorization" => auth], JSON3.write([x, y])
    )

    @test_throws HTTP.Exceptions.StatusError HTTP.get(
        "https://127.0.0.1:9000/myservice", ["Authorization" => auth], JSON3.write([x, y])
    )

    # send only the component name for a registered component
    auth = basic_auth("$authenticated_component")
    @test_throws HTTP.Exceptions.StatusError HTTP.post(
        "https://127.0.0.1:9000/mytopic", ["Authorization" => auth], JSON3.write([x, y])
    )

    @shutdown
    remove_keys(authenticated_component)
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_https"
else
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        execute(run, "test_https", secure=true, http=9000)
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end
