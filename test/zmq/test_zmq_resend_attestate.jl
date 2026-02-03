include("../utils.jl")
include("../../tools/tenant.jl")

broker_name = "zmq_resend_attestate"
pin = "11223344"
node_url = "zmq://:8012/zmq_resend_attestate_node"

function init(pin)
    add_tenant(".", pin, broker_name)
end

@info "[zmq_resend_attestate] start"
try
    zmq_ping_interval!(3)
    challenge_timeout!(2)

    init(pin)
    url = Rembus.RbURL(node_url)
    private_key = joinpath(Rembus.rembus_dir(), url.id, ".secret")
    public_key = joinpath(Rembus.broker_dir(broker_name), "keys", "$(url.id).rsa.pem")

    @info "deleting $private_key"
    rm(private_key, force=true)
    rm(public_key, force=true)

    rb = broker(zmq=8012, name=broker_name)
    Rembus.islistening(rb.router, protocol=[:zmq], wait=20)
    register(node_url, pin)
    auth_node = connect(Rembus.RbURL(node_url), name="component")
    named_node = connect("zmq://:8012/zmq_resend_named_component")
    Rembus.reset_probe!(auth_node)

    ver = rpc(auth_node, "version")
    @test ver == Rembus.VERSION

    # shutdown the broker
    shutdown(rb)
    sleep(1)

    rb = broker(zmq=8012, name=broker_name)
    sleep(3)
    Rembus.probe_pprint(auth_node)
catch e
    @error "[zmq_resend_attestate] error: $e"
    @test false
finally
    shutdown()
end
@info "[zmq_resend_attestate] end"
