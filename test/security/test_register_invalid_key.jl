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

function pubkey_of(type, bits)
    file = joinpath(Rembus.rembus_dir(), "register_invalid_key.tmpkey")
    rm(file, force=true)
    Base.run(`ssh-keygen -q -f $file -t $type -m PEM -b $bits -N ''`)
    try
        return read(`ssh-keygen -f $file -e -m PEM`)
    finally
        rm(file, force=true)
        rm("$file.pub", force=true)
    end
end

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
        (pubkey_of("rsa", 1024), Rembus.SIG_RSA),
        (pubkey_of("ecdsa", 384), Rembus.SIG_ECDSA),
        (Vector{UInt8}(codeunits(SECP256K1_PUBKEY)), Rembus.SIG_ECDSA),
        (pubkey_of("ecdsa", 256), Rembus.SIG_RSA),
        (pubkey_of("rsa", 3072), Rembus.SIG_ECDSA),
    ]
    for (pubkey, type) in invalid
        res = send_register(twin, cid, pubkey, type)
        @test res.status == Rembus.STS_GENERIC_ERROR
        @test !isfile(keyfile)
    end

    res = send_register(twin, cid, pubkey_of("ecdsa", 256), Rembus.SIG_ECDSA)
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
