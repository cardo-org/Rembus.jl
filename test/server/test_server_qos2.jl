include("../utils.jl")

# RbURL -> Broker -> Server
# The RbURL sends a QOS2 Pub/Sub message to the Broker.
# The Broker broadcasts the message to the Server.


function mytopic(ctx, rb)
    @info "[test_server_qos2]: message mytopic received"
    ctx.count += 1
end

function myservice(ctx, rb)
    @info "[test_server_qos2]: myservice"
    return 100
end

function start_server(ctx)
    s1 = server(ctx, zmq=9002, log="info")
    subscribe(s1, mytopic)
    expose(s1, myservice)

    return s1
end

function start_broker()
    #url = "zmq://127.0.0.1:9002/myserver"
    url = "zmq://127.0.0.1:9002/myserver"
    router = broker(wait=false, name=BROKER_NAME, reset=true, zmq=8002, ws=8000, log="info")
    add_node(router, url)
end

mutable struct Ctx
    count::Int
end

function run()
    ctx = Ctx(0)
    srv = start_server(ctx)

    start_broker()

    rb = component("zmq://127.0.0.1:8002")
    isconn = isconnected(rb)
    @info "isconnected: $isconn"

    publish(rb, "mytopic", qos=QOS2)
    sleep(1)

    shutdown(rb)
    shutdown(srv)
    sleep(1)
    @test ctx.count == 1
end

@info "[test_server_qos2] start"
try
    mkpath(Rembus.rembus_dir())
    run()
catch e
    @error "[test_server_qos2] error: $e"
    @test false
finally
    shutdown()
end
@info "[test_server_qos2] stop"