include("../utils.jl")

# tests: 17

using DataFrames

function init(tenant, pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(
        pin=String[pin], tenant=String[tenant], name=["Test"], enabled=Bool[true]
    )
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run(url)
    cmp = Rembus.Component(url)

    # trigger a request timeout
    ENV["REMBUS_TIMEOUT"] = 0.0
    @test_throws RembusTimeout Rembus.register(
        url, pin, tenant=tenant, scheme=Rembus.SIG_ECDSA
    )
    delete!(ENV, "REMBUS_TIMEOUT")
    sleep(0.5)
    remove_keys(cmp.id)

    Rembus.register(url, pin, tenant=tenant, scheme=Rembus.SIG_ECDSA)

    # private key was created
    @info "[test_register#1]"
    @test isfile(Rembus.pkfile(cmp.id))

    try
        Rembus.register(url, pin, tenant=tenant)
    catch e
        @info "[test_register] expected error: $e"
    end

    # move private key to test the case another client try to register
    # with the same name
    pkfile = Rembus.pkfile(cid)
    mv(pkfile, "$pkfile.staged", force=true)
    try
        # register again
        Rembus.register(url, pin, tenant=tenant, scheme=Rembus.SIG_RSA)
        @test false
    catch e
        @info "[test_register#2] expected error: $e"
        @test true
    end
    mv("$pkfile.staged", pkfile, force=true)

    # check configuration
    # component_owner file contains the component component
    df = Rembus.load_token_app(BROKER_NAME)
    @info "[test_register#3,4] component_owner: $df"
    @test df[df.component.==cmp.id, :component][1] === cmp.id
    @test df[df.component.==cmp.id, :tenant][1] === tenant

    # public key was provisioned
    fname = Rembus.pubkey_file(BROKER_NAME, cmp.id)
    @info "[test_register#5]"
    @test basename(fname) === "$(cmp.id).ecdsa.pem"
end

tenant = "A"
cid = "ecdsa_comp"
pin = "11223344"

setup() = init(tenant, pin)
try
    execute(() -> run(cid), "test_register_ecdsa", setup=setup)
catch e
    @error "[test_register_ecdsa]: $e"
    @test false
finally
    remove_keys(cid)
end
