using DataFrames

mutable struct TypeHolder
    valuemap::Vector
    TypeHolder() = new([
        Rembus.SmallInteger(UInt(1)),
        Rembus.SmallInteger(2),
        Rembus.SmallInteger(-3),
        BigInt(1),
        BigInt(-1),
        Int8(1),
        UInt8(255),
        UInt16(256),
        UInt32(65536),
        UInt64(1),
        Int8(-1),
        Int16(-129),
        Int32(-40000),
        Int64(-1),
        Float16(1.0),
        Float32(1.1),
        Float64(1.2),
        DataFrame(:a => [1, 2]),
        "foo",
        Dict(1 => 2),
        [[1, 2]],
        [Rembus.UndefLength([1, 2])],
        Undefined()
    ])
end

precompile_topic(x; ctx, node) = ctx[rid(node)] = x
precompile_service(x; ctx, node) = x
precompile_service(x, y; ctx, node) = x + y

function rpc_api(url)
    ctx = Dict()
    srv = connect("$url/precompile_server")
    expose(srv, precompile_service)
    inject(srv, ctx)

    cli = connect("$url/precompile_client")
    rpc(cli, "precompile_service", 1)
    direct(cli, "precompile_server", "precompile_service", (1, 2))
    unexpose(srv, precompile_service)
    close(cli)
    close(srv)
end

function pubsub(pub_url, sub_url)
    ctx = Dict()
    sub = connect(sub_url)
    subscribe(sub, precompile_topic)
    inject(sub, ctx)
    reactive(sub)
    pub = connect(pub_url)
    val = 1
    publish(pub, "precompile_topic", val)
    publish(pub, "precompile_topic", val, qos=Rembus.QOS2)
    unsubscribe(sub, precompile_topic)
    close(sub)
    close(pub)
end

foo(df) = df

function pool()
    df = DataFrame(:a => 1:3)
    server = connect("server")
    expose(server, foo)

    nodes = ["a", "b"]
    for policy in ["round_robin", "less_busy"]
        rb = component(nodes, policy=policy)
        for round in 1:3
            rpc(rb, "foo", df)
        end
        close(rb)
    end
    close(server)
end

function bar(n; ctx, node)
    return n
end

function alltypes(client, server)
    bag = TypeHolder()

    exposer = component(server)
    requestor = component(client)

    inject(exposer, bag)
    expose(exposer, bar)

    for msg in bag.valuemap
        rpc(requestor, "bar", msg)
    end
end

pool()
rpc_api("ws://:8000")

for pub_url in ["tcp://:8001/pub", "ws://:8000/pub"]
    for sub_url in ["tcp://:8001/tcpsub", "ws://:8000/wssub"]
        pubsub(pub_url, sub_url)
    end
end

rpc_api("tcp://:8001")
rpc_api("zmq://:8002")
alltypes("zmq://:8002/client", "zmq://:8002/server")
