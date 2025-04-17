include("../utils.jl")

broker_name = "authenticated"
node = "authenticated_mynode"

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

    settings = Dict("connection_mode" => "authenticated")
    open(joinpath(Rembus.rembus_dir(), node, "settings.json"), "w") do f
        write(f, JSON3.write(settings))
    end
end

function run()
    test_disconnection()
    test_errors()
end

function test_disconnection()
    rb = connect()
    sleep(4)
    @test !isopen(rb)
end

function test_errors()
    router = from("$broker_name.broker").args[1]
    # Connect anonymously and try to send a command.
    router.settings.request_timeout = 2

    rb = connect()
    @test_throws RembusTimeout reactive(rb)
    router.settings.request_timeout = 20

    rb = connect(node)
    @test isopen(rb)
    shutdown(rb)

    router.settings.request_timeout = 0.001
    @info "TIMEOUT=$(router.settings.request_timeout)"

    try
        connect(node)
    catch e
        @test isa(e, RembusTimeout)
    end

    # Modify the server secret.
    fn = joinpath(Rembus.broker_dir(broker_name), "keys", node)
    open(fn, "w") do f
        write(f, "wrongsecret")
    end

    shutdown(rb)

    # A wrong secret throws a RembusError
    @test_throws RembusError connect(node)

    # Anonymous connection is not permitted in authenticated mode.
    rb = connect()
    # after CHALLENGE_TIMEOUT seconds the connection is closed.
    sleep(rb.router.settings.challenge_timeout + 0.1)
    @test !isopen(rb)

    rb = component("ws://:8000", failovers=["myfailover"])
    sleep(rb.router.settings.challenge_timeout + 0.1)
    @test !isopen(rb)

    # reset the default cid
    Rembus.localcid!("")

end

execute(run, broker_name, ws=8000, authenticated=true, setup=setup)
