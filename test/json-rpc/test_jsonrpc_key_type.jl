include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "test_jsonrpc_key_type"
pin = "11223344"

function init(pin)
    add_tenant(".", pin, broker_name)
end

# Fixed SPKI public keys, so the test does not depend on the ssh-keygen
# flavour of the platform.
const RSA_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MIIBojANBgkqhkiG9w0BAQEFAAOCAY8AMIIBigKCAYEAyogKkdGNNl1mTpS+gXFK
e2l9rhU/kLyisLPRECogngJX4TELMiS1eoDtO5h+FaJpCuR3SEGXC8AoSmtr2qbN
eMWjF2YYo9krvmC45lHoISmp0Z0TlmdHA8fVVu9o9wSQZFEeXUBnbP6DQd3g7gct
3aAYFJuckoBlIEFfR3smpB5j9wjGtOhQ3w33EiN63LH7WzPsiKxxAGtihN0E04p1
bYrQ3I6s29vbziftq7SnpCxwoqOaQwDh0VJgaRM9/orswhfgwjkNG3C1NXxrrb8I
UeNrEqWicv3OYrdGySf5Y56a0ApZTQ0TxUgw0qkWbPOQOh91Tuybj0r21H56RM16
9B2+Cq36hWSoDmbPT9Wdgtq1qORLkl7FiZSn9WAWge+/aICc4Xo5MUSipcAYicE9
SAh+C5RIHnBlrSyANRRMTg4KXybD4uzxxsLr8n5pjl8I22H84nMFJ53gc2oCgPbT
pLfjonWRGqp+JkF2Ja1V+kgNwPudKR0cx83Mc7OIiUyFAgMBAAE=
-----END PUBLIC KEY-----
"""

const ECDSA_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/1RCNQXl1+t6Co6lyfBty74eWm7B
32ftZxmQKkaU9+p5oUqfWzomJAlMuD1MQ09WkyfrANoVqpCWo6bZpmKuJw==
-----END PUBLIC KEY-----
"""

pubkey_bytes(pem) = Vector{UInt8}(codeunits(pem))

# Send a Register/Rekey JSON-RPC request without the `key_type` member.
function send_without_key_type(twin, ptype, cid, pubkey)
    msg = Rembus.Rekey(
        twin, Rembus.id(), cid, Rembus.pin_proof(pin, cid), pubkey, Rembus.SIG_RSA
    )
    req = Rembus.FutureResponse(msg, Timer(20))
    twin.socket.direct[msg.id] = req
    pkt = JSON3.write(Dict(
        "rembus" => "1.0",
        "id" => string(msg.id),
        "method" => cid,
        "params" => Dict(
            "type" => ptype,
            "pin" => Rembus.base64encode(msg.pin),
            "key_val" => Rembus.base64encode(pubkey),
        )
    ))
    Rembus.ws_write(twin.socket.sock, pkt)
    return fetch(req.future)
end

function run()
    cid = "jsonrpc_key_type_node"
    keyfile = joinpath(Rembus.broker_dir(broker_name), "keys", cid)
    rm(keyfile, force=true)
    rm(joinpath(Rembus.rembus_dir(), cid, ".secret"), force=true)

    rsa = pubkey_bytes(RSA_PUBKEY)
    ecdsa = pubkey_bytes(ECDSA_PUBKEY)

    twin = connect("ws://:8338", enc=Rembus.JSON)
    # absent key_type defaults to RSA: mismatch with an ECDSA key
    res = send_without_key_type(twin, Rembus.TYPE_REGISTER, cid, ecdsa)
    @test res.status == Rembus.STS_GENERIC_ERROR
    @test !isfile(keyfile)
    res = send_without_key_type(twin, Rembus.TYPE_REGISTER, cid, rsa)
    @test res.status == Rembus.STS_SUCCESS
    @test isfile(keyfile)
    close(twin)

    # Rekey on an authenticated component registered through the regular API
    node = "jsonrpc_key_type_auth"
    private_key = joinpath(Rembus.rembus_dir(), node, ".secret")
    stored = joinpath(Rembus.broker_dir(broker_name), "keys", node)
    rm(private_key, force=true)
    rm(stored, force=true)
    register(node, pin, scheme=Rembus.SIG_RSA, enc=Rembus.JSON)
    before = read(stored)

    rb = connect(node, enc=Rembus.JSON)
    res = send_without_key_type(rb, Rembus.TYPE_REKEY, node, ecdsa)
    @test res.status == Rembus.STS_GENERIC_ERROR
    @test read(stored) == before
    res = send_without_key_type(rb, Rembus.TYPE_REKEY, node, rsa)
    @test res.status == Rembus.STS_SUCCESS
    @test read(stored) == rsa
    close(rb)
end

execute(run, broker_name, setup=() -> init(pin))
