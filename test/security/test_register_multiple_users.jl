include("../utils.jl")

# tests: 17

using DataFrames

broker_name = "register_multiple_users"

function multiple_users(tenant, pin)
    broker_dir = Rembus.broker_dir(broker_name)
    df = DataFrame(
        pin=String[pin, pin],
        tenant=String[tenant, tenant],
        name=["Test 1", "Test 2"],
        enabled=Bool[true, false]
    )
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run(url)
    nouid = "unkown_foo"
    try
        Rembus.register(url, pin, tenant=nouid)
    catch e
        @info "[register_multiple_users] expected error: $e"
        @test e.reason === "tenant [$nouid] not enabled"
    end

    try
        Rembus.register(url, pin, tenant=tenant)
    catch e
        @info "[register_multiple_users] expected error: $e"
        @test e.reason === "tenant [$tenant] not enabled"
    end
end

tenant = "A"
cid = "regcomp"
pin = "11223344"

setup() = multiple_users(tenant, pin)
try
    execute(() -> run(cid), "register_multiple_users", setup=setup)
catch e
    @error "[register_multiple_users]: $e"
    @test false
finally
    remove_keys(broker_name, cid)
end
