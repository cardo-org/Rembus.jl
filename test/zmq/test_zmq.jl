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

function run(cid)
    Rembus.islistening(wait=5, procs=["$BROKER_NAME.serve_zeromq"])
    component1 = connect(cid)
    v = rpc(component1, "version")
    @test v == Rembus.VERSION

    broker = from("$BROKER_NAME")
    shutdown(broker)
    sleep(2)

    # restart broker
    # trigger a resend_attestate()
    Rembus.broker(wait=false, name=BROKER_NAME, zmq=8002)
    sleep(2)
end

ENV["REMBUS_BASE_URL"] = "zmq://localhost:8002"
ENV["REMBUS_ZMQ_PING_INTERVAL"] = 0.5

authenticated_cid = "component1"
named_cid = "component2"

init(authenticated_cid)
#setup() = init(authenticated_cid)
#execute(() -> run(authenticated_cid), "test_zmq::1", setup=setup)

rembus_timeout = Rembus.request_timeout()
ENV["REMBUS_TIMEOUT"] = 0.25
execute(() -> run(authenticated_cid), "test_zmq::2")
execute(() -> run(named_cid), "test_zmq::2")
ENV["REMBUS_TIMEOUT"] = rembus_timeout

delete!(ENV, "REMBUS_BASE_URL")
delete!(ENV, "REMBUS_ZMQ_PING_INTERVAL")

remove_keys(authenticated_cid)
