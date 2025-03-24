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
    # Connect anonymously and try to send a command.
    rb = connect()
    Rembus.request_timeout!(2)
    @test_throws RembusTimeout reactive(rb)
    Rembus.request_timeout!(20)

    Rembus.authenticated!()
    rb = connect(node)
    @test isopen(rb)
    shutdown(rb)

    Rembus.request_timeout!(0.001)

    try
        connect(node)
    catch e
        @test isa(e, RembusTimeout)
    end

    Rembus.request_timeout!(20)

    # Modify the server secret.
    fn = joinpath(Rembus.broker_dir(broker_name), "keys", node)
    open(fn, "w") do f
        write(f, "wrongsecret")
    end

    shutdown(rb)

    # A wrong secret throws a RembusError
    @test_throws RembusError connect(node)

    # Anonymous connection is not permitted in authenticated mode.
    @test_throws ErrorException connect()

    # reset the default cid
    Rembus.localcid!("")

end

execute(run, broker_name, ws=8000, authenticated=true, setup=setup)
Rembus.anonymous!()
