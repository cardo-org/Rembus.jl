include("../utils.jl")

# set the shared secret
function init(cid)
    # component side
    pkfile = Rembus.pkfile(cid)
    open(pkfile, create=true, write=true) do f
        write(f, "aaa")
    end

    # broker side
    kdir = Rembus.keys_dir(BROKER_NAME)
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_base(BROKER_NAME, cid)
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
    # trigger a resend_attestate()
    Rembus.caronte(wait=false, name=BROKER_NAME, zmq=8002)
    sleep(2)
end

ENV["REMBUS_BASE_URL"] = "zmq://localhost:8002"
ENV["REMBUS_ZMQ_PING_INTERVAL"] = 0.5

cid1 = "component1"
setup() = init(cid1)
execute(run, "test_zmq::1", setup=setup)

rembus_timeout = Rembus.request_timeout()
ENV["REMBUS_TIMEOUT"] = 0.5
execute(run, "test_zmq::2")
ENV["REMBUS_TIMEOUT"] = rembus_timeout

delete!(ENV, "REMBUS_BASE_URL")
delete!(ENV, "REMBUS_ZMQ_PING_INTERVAL")

remove_keys(cid1)
