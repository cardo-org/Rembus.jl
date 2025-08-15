include("../utils.jl")

broker_name = "test_jsonrpc_register"
pin = "11223344"

function init(pin)
    broker_dir = Rembus.broker_dir(broker_name)
    if !isdir(broker_dir)
        mkpath(broker_dir)
    end

    tenant_settings = Dict("." => pin)
    Rembus.save_tenants(broker_dir, tenant_settings)
end

function run()
    url = "jsonrpc_register_node"
    node = Rembus.RbURL(url)
    private_key = joinpath(Rembus.rembus_dir(), rid(node), ".secret")
    public_key = joinpath(Rembus.broker_dir(broker_name), "keys", "$(rid(node)).ecdsa.pem")

    rm(private_key, force=true)
    rm(public_key, force=true)

    register(url, pin, scheme=Rembus.SIG_ECDSA, enc=Rembus.JSON)

    @test isfile(private_key)
    @test isfile(public_key)

    #    try
    #        # Triggers an invalid token error
    #        register("another_comp", "00000000")
    #        @test false
    #    catch e
    #        @info "expected error: $e"
    #        @test true
    #    end
    rb = connect(node, enc=Rembus.JSON)

    result = rpc(rb, "version")
    @info "[register] response: $result"

    unregister(rb)

    # simulate a timeout
    rb.router.settings.request_timeout = 0.0

end

execute(run, broker_name, setup=() -> init(pin))
