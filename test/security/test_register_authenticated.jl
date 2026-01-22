include("../utils.jl")

broker_name = "register_authenticated"
pin = "11223344"

function init(pin)
    broker_dir = Rembus.broker_dir(broker_name)
    if !isdir(broker_dir)
        mkpath(broker_dir)
    end

    tenant_settings = Dict("." => pin)
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_name, tenant_settings)
end

function run()
    for node in [
        "register_authenticated_node",
        "zmq://:8002/register_authenticated_zmqnode"
    ]
        reg(node)
    end
end

function reg(url)
    node = Rembus.RbURL(url)
    private_key = joinpath(Rembus.rembus_dir(), rid(node), ".secret")
    public_key = joinpath(Rembus.broker_dir(broker_name), "keys", "$(rid(node)).rsa.pem")

    rm(private_key, force=true)
    rm(public_key, force=true)

    register(url, pin)

    @test isfile(private_key)
    @test isfile(public_key)

    try
        register(url, pin)
        @test false
    catch e
        @info "expected error: $e"
        @test true
    end
    rb = connect(node)

    result = rpc(rb, "version")
    @info "[test_register_authenticated] response: $result"

    unregister(rb)

end

execute(
    run,
    broker_name,
    ws=8000,
    zmq=8002,
    authenticated=true,
    setup=() -> init(pin)
)
