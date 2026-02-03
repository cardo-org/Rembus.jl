include("../utils.jl")
include("../../tools/tenant.jl")

# tests: 17

using DataFrames

broker_name = "register_multiple_users"

function multiple_users(pin)
    for tenant in ["t1", "t2"]
        add_tenant(tenant, pin, broker_name)
    end

end

function run(cid)
    tenant = Rembus.domain(cid)
    try
        Rembus.register(cid, pin)
    catch e
        @info "[register_multiple_users] expected error: $e"
        @test e.reason === "tenant [$tenant] not enabled"
    end
end

cid = "regcomp.t3"
pin = "11223344"

setup() = multiple_users(pin)
try
    execute(() -> run(cid), "register_multiple_users", setup=setup)
catch e
    @error "[register_multiple_users]: $e"
    @test false
finally
    remove_keys(broker_name, cid)
end
