include("../utils.jl")

# set the shared secret
function init(cid)
    # component side
    pkfile = Rembus.pkfile(cid)
    open(pkfile, create=true, write=true) do f
        write(f, "aaa")
    end

    # broker side
    kdir = Rembus.keys_dir()
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_file(cid)
    open(fn, create=true, write=true) do f
        write(f, "aaa")
    end
end

function run()
    component1 = connect(cid1)

    v = rpc(component1, "version")
    @test v == Rembus.VERSION

    shutdown()
    sleep(2)

    # restart caronte
    Rembus.caronte(wait=false, exit_when_done=false)
    sleep(2)

    close(component1)
end

ENV["REMBUS_BASE_URL"] = "zmq://localhost:8002"
ENV["REMBUS_ZMQ_PING_INTERVAL"] = 0.1

cid1 = "component1"
setup() = init(cid1)
execute(run, "test_zmq", setup=setup)
delete!(ENV, "REMBUS_BASE_URL")
delete!(ENV, "REMBUS_ZMQ_PING_INTERVAL")
remove_keys(cid1)
