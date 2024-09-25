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
    @test_throws RembusTimeout Rembus.register(url, uid, pin, Rembus.SIG_ECDSA)
    delete!(ENV, "REMBUS_TIMEOUT")
    sleep(0.5)
    remove_keys(cmp.id)

    Rembus.register(url, uid, pin, Rembus.SIG_ECDSA)

    # private key was created
    @info "[test_register#1]"
    @test isfile(Rembus.pkfile(cmp.id))

    try
        Rembus.register(url, uid, pin)
    catch e
        @info "[test_register] expected error: $e"
    end

    # move private key to test the case another client try to register
    # with the same name
    pkfile = Rembus.pkfile(cid)
    mv(pkfile, "$pkfile.staged", force=true)
    try
        # register again
        Rembus.register(url, uid, pin, Rembus.SIG_RSA)
        @test false
    catch e
        @info "[test_register#2] expected error: $e"
        showerror(stdout, e, catch_backtrace())
        @test true
    end
    mv("$pkfile.staged", pkfile, force=true)

    # check configuration
    # component_owner file contains the component component
    df = Rembus.load_token_app(BROKER_NAME)
    @info "[test_register#3,4] component_owner: $df"
    @test df[df.component.==cmp.id, :component][1] === cmp.id
    @test df[df.component.==cmp.id, :uid][1] === uid

    # public key was provisioned
    fname = Rembus.pubkey_file(BROKER_NAME, cmp.id)
    @info "[test_register#5]"
    @test basename(fname) === "$(cmp.id).ecdsa.pem"
end

uid = "ecdsa_user"
cid = "ecdsa_comp"
pin = "11223344"

setup() = init(uid, pin)
try
    execute(() -> run(cid), "test_register_ecdsa", setup=setup)
catch e
    @error "[test_register_ecdsa]: $e"
    @test false
finally
    remove_keys(cid)
end
