include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "rekey"
pin = "11223344"

function init(pin)
    add_tenant(".", pin, broker_name)
end

function send_rekey(twin, cid, proof, pubkey, type)
    msg = Rembus.Rekey(twin, Rembus.id(), cid, proof, pubkey, type)
    return fetch(Rembus.send_msg(twin, msg).future)
end

function run()
    for node in ["rekey_node", "zmq://:8336/rekey_zmqnode"]
        rk(node)
    end
end

function rk(url)
    node = Rembus.RbURL(url)
    private_key = joinpath(Rembus.rembus_dir(), rid(node), ".secret")
    keys = joinpath(Rembus.broker_dir(broker_name), "keys")
    key = joinpath(keys, "$(rid(node))")
    legacy_key = "$key.rsa.pem"

    rm(private_key, force=true)
    rm(key, force=true)
    rm(legacy_key, force=true)

    register(url, pin)
    @test isfile(key)
    @test !isfile(legacy_key)
    old_pubkey = read(key)
    old_private = read(private_key)

    # wrong pin: the old key is kept
    @test_throws Rembus.RembusError rekey(url, "00000000", scheme=Rembus.SIG_ECDSA)
    @test read(private_key) == old_private
    @test read(key) == old_pubkey

    twin = connect(url)
    proof = Rembus.pin_proof(pin, rid(node))

    # invalid pubkey, cid mismatch, bad type: the old key is kept
    res = send_rekey(twin, rid(node), proof, UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11], Rembus.SIG_RSA)
    @test res.status == Rembus.STS_GENERIC_ERROR
    res = send_rekey(twin, rid(node), proof, old_pubkey, Rembus.SIG_ECDSA)
    @test res.status == Rembus.STS_GENERIC_ERROR
    res = send_rekey(twin, "someone_else", Rembus.pin_proof(pin, "someone_else"), old_pubkey, Rembus.SIG_RSA)
    @test res.status == Rembus.STS_GENERIC_ERROR
    @test read(key) == old_pubkey
    # the connection is still usable
    @test rpc(twin, "version") !== nothing
    close(twin)

    # success with a different scheme
    # ssh-keygen generated ECDSA keys are not usable on macOS (MbedTLS limitation)
    new_scheme = Sys.isapple() ? Rembus.SIG_RSA : Rembus.SIG_ECDSA
    rekey(url, pin, scheme=new_scheme)
    @test read(private_key) != old_private
    @test read(key) != old_pubkey
    @test isempty(filter(f -> startswith(f, "jl_"), readdir(keys)))

    # authentication with the new key works
    rb = connect(url)
    @test rpc(rb, "version") !== nothing
    close(rb)

    # authentication with the old key fails
    new_private = read(private_key)
    write(private_key, old_private)
    @test_throws Exception connect(url)
    write(private_key, new_private)

    # a legacy (type/format encoded) key file is still honoured and is replaced
    # by the canonical file after a successful rekey
    mv(key, legacy_key)
    rb = connect(url)
    @test rpc(rb, "version") !== nothing
    close(rb)
    rekey(url, pin)
    @test isfile(key)
    @test !isfile(legacy_key)

    rb = connect(url)
    unregister(rb)

    # unregistered component cannot rekey
    @test_throws Exception rekey(url, pin)
end

execute(
    run,
    broker_name,
    ws=8338,
    zmq=8336,
    authenticated=true,
    setup=() -> init(pin)
)
