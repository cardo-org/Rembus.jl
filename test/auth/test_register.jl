include("../utils.jl")

# tests: 17

using DataFrames

function init(pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run(url)
    cmp = Rembus.Component(url)

    # trigger a request timeout
    ENV["REMBUS_TIMEOUT"] = 0.0
    @test_throws RembusTimeout register(url, pin)
    delete!(ENV, "REMBUS_TIMEOUT")

    # wait a moment, to be sure that the pubkey file is created by the above register
    sleep(0.5)
    remove_keys(cmp.id)

    register(url, pin)

    # private key was created
    @info "[test_register#1]"
    @test isfile(Rembus.pkfile(cmp.id))

    try
        register(url, pin)
    catch e
        @info "[test_register] expected error: $e"
    end

    try
        # wrong token
        register("mycomponent", "00000000")
        @test false
    catch e
        @info "[test_register#2] expected error: $e"
        @test true
    end

    # move private key to test the case another client try to register
    # with the same name
    pkfile = Rembus.pkfile(cid)
    mv(pkfile, "$pkfile.staged", force=true)
    try
        # register again
        register(url, pin)
        @test false
    catch e
        @info "[test_register#3] expected error: $e"
        @test true
    end
    mv("$pkfile.staged", pkfile, force=true)

    # check configuration
    # component_owner file contains the component component
    df = Rembus.load_token_app(BROKER_NAME)
    @info "[test_register#4,5] component_owner: $df"
    @test df[df.component.==cmp.id, :component][1] === cmp.id
    @test df[df.component.==cmp.id, :tenant][1] === BROKER_NAME

    # public key was provisioned
    fname = Rembus.pubkey_file(BROKER_NAME, cmp.id)
    @info "[test_register#6]"
    @test basename(fname) === "$(cmp.id).rsa.pem"
end

function decommission(url)
    client = tryconnect("zmq://:8002/wrong_cid")

    try
        unregister(client)
        @test false
    catch e
        @info "[test_register#7] unregister expected error: $e"
        @test true
    finally
        close(client)
    end

    client = tryconnect(url)
    try
        unregister(client)

        df = Rembus.load_token_app(BROKER_NAME)

        # the component was removed from component_owner file
        @info "[test_register#8,9,10]"
        @test isempty(df[df.component.==cid, :])

        # the public key was removed
        @test_throws ErrorException Rembus.pubkey_file(BROKER_NAME, cid)

        # the private key was removed
        @test isfile(Rembus.pkfile(cid)) === false
    catch
        @test false
        rethrow()
    finally
        close(client)
    end

    client = tryconnect("public_component")
    try
        unregister(client)
        @test false
    catch e
        @info "[test_register#10] unregister expected error: $e"
        @test true
    finally
        close(client)
    end
end

cid = "regcomp"
pin = "11223344"

setup() = init(pin)
try
    url = "zmq://:8002/$cid"
    execute(() -> run(url), "test_register", setup=setup)
    execute(() -> decommission(url), "test_unregister", setup=setup)

    url = cid
    execute(() -> run(cid), "test_register", setup=setup)

catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
