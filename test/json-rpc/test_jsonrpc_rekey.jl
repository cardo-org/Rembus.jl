include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "test_jsonrpc_rekey"
pin = "11223344"

function init(pin)
    add_tenant(".", pin, broker_name)
end

function run()
    url = "jsonrpc_rekey_node"
    node = Rembus.RbURL(url)
    private_key = joinpath(Rembus.rembus_dir(), rid(node), ".secret")
    public_key = joinpath(Rembus.broker_dir(broker_name), "keys", "$(rid(node))")

    rm(private_key, force=true)
    rm(public_key, force=true)

    register(url, pin, scheme=Rembus.SIG_RSA, enc=Rembus.JSON)
    old_private = read(private_key)
    old_public = read(public_key)

    # wrong pin: error response, old key kept
    @test_throws Rembus.RembusError rekey(
        url, "00000000", scheme=Rembus.SIG_ECDSA, enc=Rembus.JSON
    )
    @test read(private_key) == old_private
    @test read(public_key) == old_public

    # invalid public key: error response with status 10, old key kept
    twin = connect(node, enc=Rembus.JSON)
    msg = Rembus.Rekey(
        twin, Rembus.id(), rid(node), Rembus.pin_proof(pin, rid(node)),
        UInt8[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12], Rembus.SIG_RSA
    )
    res = fetch(Rembus.send_msg(twin, msg).future)
    @test res.status == Rembus.STS_GENERIC_ERROR
    @test read(public_key) == old_public
    close(twin)

    # ssh-keygen generated ECDSA keys are not usable on macOS (MbedTLS limitation)
    new_scheme = Sys.isapple() ? Rembus.SIG_RSA : Rembus.SIG_ECDSA
    rekey(url, pin, scheme=new_scheme, enc=Rembus.JSON)
    @test read(private_key) != old_private
    @test read(public_key) != old_public

    rb = connect(node, enc=Rembus.JSON)
    @test rpc(rb, "version") !== nothing
    unregister(rb)
end

execute(run, broker_name, setup=() -> init(pin))
