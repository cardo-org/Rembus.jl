include("../utils.jl")

# tests: 17

using DataFrames

function disabled_user(tenant, pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin], tenant=String[tenant], enabled=Bool[false])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run(url)
    try
        Rembus.register(url, pin, tenant="A")
    catch e
        @info "[test_register] expected error: $e"
        @test e.reason === "tenant [$tenant] not enabled"
    end
end

tenant = "A"
cid = "regcomp"
pin = "11223344"

setup() = disabled_user(tenant, pin)
try
    execute(() -> run(cid), "test_register", setup=setup)
catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
