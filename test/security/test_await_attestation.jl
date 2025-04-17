include("../utils.jl")

broker_name = "await_attestation"
node = "await_attestation_mynode"

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
    # Set a zero timeout that triggers an error when waiting for
    # the Attestation from the connecting node.
    router = from("$broker_name.broker").args[1]
    router.settings.request_timeout = 0.0000

    #@test_throws RembusError connect(node)
    @test_throws RembusError connect(node)
end

execute(run, broker_name, ws=8000, authenticated=true, setup=setup)
