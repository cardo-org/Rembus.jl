include("../utils.jl")

using DataFrames

function init(uid, pin)
    df = DataFrame(pin=String[pin], uid=String[uid], name=["Test"], enabled=Bool[true])
    if !isdir(Rembus.CONFIG.db)
        mkdir(Rembus.CONFIG.db)
    end
    Rembus.save_owners(df)
end

function run()
    cmp = Rembus.Component(url)

    Rembus.register(url, uid, pin)

    # private key was created
    @test isfile(Rembus.pkfile(cmp.id))

    try
        # wrong token
        Rembus.register("mycomponent", uid, "00000000")
        @test false
    catch e
        @info "[test_register] expected error: $e"
        @test true
    end

    # move private key to test the case another client try to register
    # with the same name
    pkfile = Rembus.pkfile(cid)
    mv(pkfile, "$pkfile.staged")
    try
        # register again
        Rembus.register(url, uid, pin)
        @test false
    catch e
        @info "[test_register] expected error: $e"
        @test true
    end
    mv("$pkfile.staged", pkfile)

    # check configuration
    # component_owner file contains the component component
    df = Rembus.load_token_app()
    @debug "component_owner: $df" _group = :test
    @test df[df.component.==cmp.id, :component][1] === cmp.id
    @test df[df.component.==cmp.id, :uid][1] === uid


    # public key was provisioned
    fname = Rembus.pubkey_file(cmp.id)
    @test basename(fname) === cmp.id
end

function run_embedded()
    try
        rb = embedded()
        serve(rb, wait=false, exit_when_done=false)

        client = connect(cid)
        close(client)
    catch e
        @error "run_embedded: $e"
        @test false
    finally
        shutdown()
    end

end

function unregister()
    client = tryconnect(url)

    try
        Rembus.unregister(client, "wrong_cid")
        @test false
    catch e
        @info "[unregister]: expected error: $e"
        @test true
    end

    try
        Rembus.unregister(client, cid)

        df = Rembus.load_token_app()

        # the component was removed from component_owner file
        @test isempty(df[df.component.==cid, :])

        # the public key was removed
        @test_throws ErrorException Rembus.pubkey_file(cid)

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
        Rembus.unregister(client, "some_cid")
        @test false
    catch e
        @info "[unregister]: expected error: $e"
        @test true
    finally
        close(client)
    end

end



uid = "rembus_user"
cid = "regcomp"
url = "zmq://:8002/$cid"
pin = "11223344"

setup() = init(uid, pin)
try
    execute(run, "test_register", setup=setup)

    @info "[test_authenticated_embedded] start"
    run_embedded()

    execute(unregister, "test_unregister", setup=setup)
catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
