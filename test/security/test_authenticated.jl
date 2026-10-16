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
    test_loopback()
    test_errors()
end

function remote_address()
    ip = Rembus.Sockets.getipaddr()
    return (ip.host >> 24) == 127 ? nothing : string(ip)
end

function test_loopback()
    router = from("$broker_name.broker").args[1]

    # Anonymous components connected from the loopback interface are permitted
    # and are not disconnected after the challenge timeout.
    rb = connect()
    sleep(router.settings.challenge_timeout + 0.1)
    @test isopen(rb)
    @test reactive(rb) === nothing
    shutdown(rb)

    # Anonymous components connected from other addresses are disconnected.
    addr = remote_address()
    if addr === nothing
        @warn "no non-loopback address available: skipping remote test"
    else
        router.settings.request_timeout = 2
        rb = component("ws://$addr:8338")
        @test_throws RembusTimeout reactive(rb)
        router.settings.request_timeout = 20
        sleep(router.settings.challenge_timeout + 0.1)
        @test !isopen(rb)
    end
end

function test_errors()
    router = from("$broker_name.broker").args[1]
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

    # reset the default cid
    Rembus.localcid!("")

end

execute(run, broker_name, ws=8338, authenticated=true, setup=setup)
