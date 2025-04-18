include("../utils.jl")

broker_name = "challenge"
node = "challenge_mynode"

function Rembus.transport_send(socket::Rembus.AbstractPlainSocket, msg::Rembus.Attestation)
    # does not send the attestation
    return true
end


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

    settings = Dict("connection_mode" => "authenticated", "request_timeout" => 1)
    open(joinpath(Rembus.rembus_dir(), node, "settings.json"), "w") do f
        write(f, JSON3.write(settings))
    end

end

function run()
    @test_throws RembusTimeout connect(node)
end

execute(run, "challenge", authenticated=true, ws=8000, setup=setup)

function Rembus.transport_send(socket::Rembus.AbstractPlainSocket, msg::Rembus.Attestation)
    pkt = [Rembus.TYPE_ATTESTATION, Rembus.id2bytes(msg.id), msg.cid, msg.signature, msg.meta]
    Rembus.transport_write(socket, pkt)
    return true
end
