include("../utils.jl")

# tests: 17

using DataFrames

function multiple_users(uid, pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin, pin], uid=String[uid, uid], name=["Test 1", "Test 2"], enabled=Bool[true, false])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_owners(broker_dir, df)
end

function run(url)
    nouid = "unkown_foo"
    try
        Rembus.register(url, nouid, pin)
    catch e
        @info "[test_register] expected error: $e"
        @test e.reason === "user [$nouid] not enabled"
    end

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

setup() = multiple_users(uid, pin)
try
    execute(() -> run(cid), "test_register", setup=setup)
catch e
    @error "[test_register]: $e"
    @test false
finally
    remove_keys(cid)
end
