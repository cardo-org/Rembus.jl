include("../utils.jl")

# tests: 17

using DataFrames

function disabled_user(uid, pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin], uid=String[uid], enabled=Bool[false])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_owners(broker_dir, df)
end

function run(url)
    try
        Rembus.register(url, uid, pin)
    catch e
        @info "[test_register] expected error: $e"
        @test e.reason === "user [$uid] not enabled"
    end
end

uid = "rembus_user"
cid = "regcomp"
pin = "11223344"

setup() = disabled_user(uid, pin)
try
    execute(() -> run(cid), "test_register", setup=setup)
catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
