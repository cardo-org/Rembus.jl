include("../utils.jl")

broker_name = "custom_login"
node = "custom_login_mynode"

function setup()
    dirs_files = [
        (joinpath(Rembus.rembus_dir(), node), ".secret"),
        (joinpath(Rembus.broker_dir(broker_name), "keys"), node)
    ]

    for (dir, filename) in dirs_files
        mkpath(dir)
        fn = joinpath(dir, filename)
        open(fn, "w") do f
            write(f, "mysecret")
        end
    end
end

function custom_login(twin, cid, signature)
    @info "[custom_login] login: $cid"
    return false
end

function run()
    request_timeout!(10)
    bro = broker(name=broker_name, ws=8000)

    bro.router.local_function["login"] = custom_login

    @test_throws RembusError connect(node)
end

@info "[custom_login] start"
try
    setup()
    run()
catch e
    @error "[custom_login] error: $e"
    @test false
finally
    shutdown()
end

@info "[custom_login] stop"
