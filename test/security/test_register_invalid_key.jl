include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "register_invalid_key"
pin = "11223344"

const SECP256K1_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MFYwEAYHKoZIzj0CAQYFK4EEAAoDQgAElGy7p1/LX05QoGnTumCsyVtVrbYVagLG
+Xaw9DB2u1AfN3ZiuBW1sA8ZhHt5F6FDhngz6GZOiTLotYCfilPKyA==
-----END PUBLIC KEY-----
"""

function init(pin)
    add_tenant(".", pin, broker_name)
end

# Fixed SPKI public keys, so the test does not depend on the ssh-keygen
# flavour of the platform.
const RSA_1024_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQDKUdJL6o/N+i7NeEsSba8zFn2D
qy0GlVzES1SGAC1Jdd8NHkyq58Ogxvxe9wgWFyJUmY2CRO8OVg876fbT+UBjwb3y
bBhiKFFYXgADSOiOe+EILTH8k3Ox72d5Vy78VPI0KnCkCPcuzRiNeRVyiUdan4r1
w5I71WgG0yNzKVmeBQIDAQAB
-----END PUBLIC KEY-----
"""

const RSA_3072_PUBKEY = """
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

const ECDSA_P256_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/1RCNQXl1+t6Co6lyfBty74eWm7B
32ftZxmQKkaU9+p5oUqfWzomJAlMuD1MQ09WkyfrANoVqpCWo6bZpmKuJw==
-----END PUBLIC KEY-----
"""

const ECDSA_P384_PUBKEY = """
-----BEGIN PUBLIC KEY-----
MHYwEAYHKoZIzj0CAQYFK4EEACIDYgAEGxdE+xNKrK0zXLxSDIJZCo5WY/eUzoQc
Xb7HSAwb+f0hZbUyzCvaGZPgSMO5nxBCAu40HBW27LMu78/ZWm0Xuapbg3EqYG4u
n3x0XSvCAzqtXcJupzaPGhNJ6U0rgSJu
-----END PUBLIC KEY-----
"""

pubkey_bytes(pem) = Vector{UInt8}(codeunits(pem))

function send_register(twin, cid, pubkey, type)
    proof = Rembus.pin_proof(pin, cid)
    msg = Rembus.Register(twin, Rembus.id(), cid, proof, pubkey, type)
    return fetch(Rembus.send_msg(twin, msg).future)
end

function run()
    cid = "register_invalid_key_node"
    keyfile = joinpath(Rembus.broker_dir(broker_name), "keys", cid)
    rm(keyfile, force=true)

    twin = connect("tcp://:8337")
    invalid = [
        (UInt8[], Rembus.SIG_RSA),
        (UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], Rembus.SIG_RSA),
        (pubkey_bytes(RSA_1024_PUBKEY), Rembus.SIG_RSA),
        (pubkey_bytes(ECDSA_P384_PUBKEY), Rembus.SIG_ECDSA),
        (pubkey_bytes(SECP256K1_PUBKEY), Rembus.SIG_ECDSA),
        (pubkey_bytes(ECDSA_P256_PUBKEY), Rembus.SIG_RSA),
        (pubkey_bytes(RSA_3072_PUBKEY), Rembus.SIG_ECDSA),
    ]
    for (pubkey, type) in invalid
        res = send_register(twin, cid, pubkey, type)
        @test res.status == Rembus.STS_GENERIC_ERROR
        @test !isfile(keyfile)
    end

    res = send_register(twin, cid, pubkey_bytes(ECDSA_P256_PUBKEY), Rembus.SIG_ECDSA)
    res.status == Rembus.STS_SUCCESS || @info "register failed" reason = res.data
    @test res.status == Rembus.STS_SUCCESS
    @test isfile(keyfile)
    close(twin)
end

execute(
    run,
    broker_name,
    tcp=8337,
    authenticated=false,
    setup=() -> init(pin)
)
