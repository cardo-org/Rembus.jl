include("../utils.jl")

using DataFrames

broker_name = "register_multitenancy"

function init(pin)
    broker_dir = Rembus.broker_dir(broker_name)
    df = DataFrame(pin=String[pin, pin], tenant=String["A", "B"], enabled=Bool[true, true])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run(url)
    cmp = Rembus.RbURL(url)

    # trigger a request timeout
    request_timeout!(0)
    @test_throws RembusTimeout Rembus.register(url, pin, tenant="A")
    request_timeout!(20)


    # wait a moment, to be sure that the pubkey file is created by the above register
    sleep(0.5)
    remove_keys(broker_name, cmp.id)

    Rembus.register(url, pin, tenant="A")

    # private key was created
    @info "[register_multitenancy#1]"
    @test isfile(Rembus.pkfile(cmp.id))

    try
        Rembus.register(url, pin, tenant="A")
    catch e
        @info "[register_multitenancy] expected error: $e"
    end

    try
        # wrong token
        Rembus.register("mycomponent", "00000000", tenant="A")
        @test false
    catch e
        @info "[register_multitenancy#2] expected error: $e"
        @test true
    end

    # move private key to test the case another client try to register
    # with the same name
    pkfile = Rembus.pkfile(cid)
    mv(pkfile, "$pkfile.staged", force=true)
    try
        # register again
        Rembus.register(url, pin, tenant="A")
        @test false
    catch e
        @info "[register_multitenancy#3] expected error: $e"
        @test true
    end
    mv("$pkfile.staged", pkfile, force=true)

    # check configuration
    # component_owner file contains the component component
    df = Rembus.load_token_app(broker_name)
    @info "[register_multitenancy#4,5] component_owner: $df"
    @test df[df.component.==cmp.id, :component][1] === cmp.id
    @test df[df.component.==cmp.id, :tenant][1] === "A"

    # public key was provisioned
    fname = Rembus.pubkey_file(broker_name, cmp.id)
    @info "[register_multitenancy#6]"
    @test basename(fname) === "$(cmp.id).rsa.pem"
end

function decommission(url)
    client = connect("zmq://:8002/wrong_cid")

    try
        Rembus.unregister(client)
        @test false
    catch e
        @info "[register_multitenancy#7] unregister expected error: $e"
        @test true
    finally
        close(client)
    end

    client = connect(url)
    try
        Rembus.unregister(client)

        df = Rembus.load_token_app(broker_name)
        # the component was removed from component_owner file
        @info "[register_multitenancy#8,9,10]"
        @test isempty(df[df.component.==cid, :])

        # the public key was removed
        @test_throws ErrorException Rembus.pubkey_file(broker_name, cid)

        # the private key was removed
        @test isfile(Rembus.pkfile(cid)) === false
    catch
        @test false
        rethrow()
    finally
        close(client)
    end

    client = connect("public_component")
    try
        Rembus.unregister(client)
        @test false
    catch e
        @info "[register_multitenancy#10] unregister expected error: $e"
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
    execute(() -> run(url), "register_multitenancy", setup=setup)
    execute(() -> decommission(url), "register_multitenancy", setup=setup)
    url = cid
    execute(() -> run(cid), "register_multitenancy", setup=setup)
catch e
    @error "[register_multitenancy]: $e"
    @test false
finally
    remove_keys(broker_name, cid)
end
