include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "register_nonce"
pin = "11223344"

const ECDSA_P256_PUBKEY = Vector{UInt8}(codeunits("""
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/1RCNQXl1+t6Co6lyfBty74eWm7B
32ftZxmQKkaU9+p5oUqfWzomJAlMuD1MQ09WkyfrANoVqpCWo6bZpmKuJw==
-----END PUBLIC KEY-----
"""))
const ECDSA = Rembus.SIG_ECDSA

function init(pin)
    add_tenant(".", pin, broker_name)
end

function request(twin, T, cid, proof, pubkey=ECDSA_P256_PUBKEY, type=ECDSA)
    msg = T(twin, Rembus.id(), cid, proof, pubkey, type)
    return fetch(Rembus.send_msg(twin, msg).future)
end

function challenge(twin, T, cid)
    res = request(twin, T, cid, UInt8[], UInt8[], 0)
    @test res.status == Rembus.STS_CHALLENGE
    return Rembus.response_data(res)
end

proof_of(cid, nonce) = Rembus.pin_proof(pin, cid, nonce, ECDSA_P256_PUBKEY, ECDSA)

function run()
    cid = "register_nonce_node"
    keyfile = joinpath(Rembus.broker_dir(broker_name), "keys", cid)
    rm(keyfile, force=true)
    Reg = Rembus.Register
    Rek = Rembus.Rekey
    ERR = Rembus.STS_GENERIC_ERROR

    twin = connect("tcp://:8337")
    other = connect("tcp://:8337")

    # a proof without outstanding nonce fails
    @test request(twin, Reg, cid, proof_of(cid, rand(UInt8, 32))).status == ERR

    # the pre-1.0 one-step proof is rejected, also after a challenge
    legacy = Rembus.MbedTLS.digest(Rembus.MD_SHA256, Rembus.encode([pin, cid]))
    @test request(twin, Reg, cid, legacy).status == ERR
    challenge(twin, Reg, cid)
    @test request(twin, Reg, cid, legacy).status == ERR

    # invalid pin length
    @test request(twin, Reg, cid, UInt8[1, 2, 3]).status == ERR

    # nonce: 32 random bytes, never reissued, new challenge invalidates the old
    n1 = challenge(twin, Reg, cid)
    n2 = challenge(twin, Reg, cid)
    @test length(n1) == 32 && length(n2) == 32 && n1 != n2
    @test request(twin, Reg, cid, proof_of(cid, n1)).status == ERR

    # the nonce is single use also on failure
    n = challenge(twin, Reg, cid)
    @test request(twin, Reg, cid, proof_of(cid, rand(UInt8, 32))).status == ERR
    @test request(twin, Reg, cid, proof_of(cid, n)).status == ERR

    # bound to the connection
    n = challenge(twin, Reg, cid)
    @test request(other, Reg, cid, proof_of(cid, n)).status == ERR

    # bound to the cid
    n = challenge(twin, Reg, cid)
    @test request(twin, Reg, "register_nonce_other", proof_of("register_nonce_other", n)).status == ERR

    # bound to the operation
    n = challenge(twin, Reg, cid)
    @test request(twin, Rek, cid, proof_of(cid, n)).status == ERR
    @test request(twin, Reg, cid, proof_of(cid, n)).status == Rembus.STS_SUCCESS
    @test isfile(keyfile)

    # the proof binds pubkey and type
    rm(keyfile, force=true)
    n = challenge(twin, Reg, cid)
    @test request(twin, Reg, cid, proof_of(cid, n), ECDSA_P256_PUBKEY, Rembus.SIG_RSA).status == ERR
    @test !isfile(keyfile)

    # expired nonce
    ttl = Rembus.REGISTER_NONCE_TTL[]
    Rembus.REGISTER_NONCE_TTL[] = 0.2
    n = challenge(twin, Reg, cid)
    sleep(0.5)
    Rembus.REGISTER_NONCE_TTL[] = ttl
    @test request(twin, Reg, cid, proof_of(cid, n)).status == ERR
    @test !isfile(keyfile)

    # success, then replay of the captured proof fails
    n = challenge(twin, Reg, cid)
    proof = proof_of(cid, n)
    @test request(twin, Reg, cid, proof).status == Rembus.STS_SUCCESS
    @test request(twin, Reg, cid, proof).status == ERR
    # a fresh challenge for a registered cid is accepted, the proof gets "name taken"
    n = challenge(twin, Reg, cid)
    @test request(twin, Reg, cid, proof_of(cid, n)).status == Rembus.STS_NAME_ALREADY_TAKEN

    # invalid cid or unauthenticated Rekey: no nonce is issued
    @test request(twin, Reg, "..bad", UInt8[], UInt8[], 0).status == ERR
    @test request(twin, Rek, cid, UInt8[], UInt8[], 0).status == ERR

    # the number of outstanding nonces per connection is bounded
    results = [
        request(other, Reg, "register_nonce_$i", UInt8[], UInt8[], 0).status
        for i in 1:(Rembus.MAX_REGISTER_NONCES+1)
    ]
    @test results[end] == ERR
    @test all(==(Rembus.STS_CHALLENGE), results[1:end-1])

    close(twin)
    close(other)
end

execute(
    run,
    broker_name,
    tcp=8337,
    authenticated=false,
    setup=() -> init(pin)
)
