include("../utils.jl")

# tests: 17

using DataFrames

function init(uid, pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin], uid=String[uid], name=["Test"], enabled=Bool[true])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_owners(broker_dir, df)
end

function run(url)
    cmp = Rembus.Component(url)

    # trigger a request timeout
    ENV["REMBUS_TIMEOUT"] = 0.0
    @test_throws RembusTimeout Rembus.register(url, uid, pin)
    delete!(ENV, "REMBUS_TIMEOUT")
    remove_keys(cmp.id)

    Rembus.register(url, uid, pin)

    # private key was created
    @info "[test_register#1]"
    @test isfile(Rembus.pkfile(cmp.id))

    try
        Rembus.register(url, uid, pin)
    catch e
        @info "[test_register] expected error: $e"
    end

    try
        # wrong token
        Rembus.register("mycomponent", uid, "00000000")
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
        Rembus.register(url, uid, pin)
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
    @test df[df.component.==cmp.id, :uid][1] === uid

    # public key was provisioned
    fname = Rembus.pubkey_file(BROKER_NAME, cmp.id)
    @info "[test_register#6]"
    @test basename(fname) === cmp.id
end

function unregister(url)
    client = tryconnect("zmq://:8002/wrong_cid")

    try
        Rembus.unregister(client)
        @test false
    catch e
        @info "[test_register#7] unregister expected error: $e"
        @test true
    finally
        close(client)
    end

    client = tryconnect(url)
    try
        Rembus.unregister(client)

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
        Rembus.unregister(client)
        @test false
    catch e
        @info "[test_register#10] unregister expected error: $e"
        @test true
    finally
        close(client)
    end

end

uid = "rembus_user"
cid = "regcomp"
pin = "11223344"

setup() = init(uid, pin)
try
    url = "zmq://:8002/$cid"
    execute(() -> run(url), "test_register", setup=setup)
    @info "[test_authenticated_embedded] start"
    execute(() -> unregister(url), "test_unregister", setup=setup)

    url = cid
    execute(() -> run(cid), "test_register", setup=setup)

catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
