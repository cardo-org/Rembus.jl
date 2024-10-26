include("../utils.jl")

using DataFrames

# tests: 3

function init(server_name, tenant, pin)
    broker_dir = Rembus.broker_dir(server_name)
    df = DataFrame(
        pin=String[pin], tenant=String[tenant], enabled=Bool[true]
    )
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end


function run()
    cid = "mycid"
    tenant = "TenantA"
    pin = "11223344"
    server_name = "server_test"

    init(server_name, tenant, pin)

    server(name=server_name)

    Rembus.register(cid, pin, tenant=tenant)

    keyfn = joinpath(Rembus.keys_dir(server_name), cid)
    @test keyfn !== nothing

    rb = connect(cid)

    @test rpc(rb, "version") == Rembus.VERSION

    Rembus.unregister(rb)

    ENV["REMBUS_TIMEOUT"] = 0
    @test_throws RembusTimeout Rembus.unregister(rb)
    delete!(ENV, "REMBUS_TIMEOUT")

    @test !isfile(keyfn)

    close(rb)
end

function login_failed()
    cid = "mycid"
    server_name = "server_test"
    kdir = Rembus.keys_dir(server_name)
    mkpath(kdir)
    server_side_secret = joinpath(kdir, "$cid.rsa.pem")
    try
        write(server_side_secret, "aaa")
        write(Rembus.pkfile(cid), "bbb")

        server(name=server_name)
        @test_throws RembusError connect(cid)
    finally
        shutdown()
    end
    rm(server_side_secret)
    rm(Rembus.pkfile(cid))

end

try
    run()
catch e
    @error "[test_register_server]; $e"
    @test false
finally
    shutdown()
end

@info "[test_register_server] start"
login_failed()
@info "[test_register_server] stop"
