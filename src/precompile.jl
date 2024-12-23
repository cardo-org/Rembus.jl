# When compiling responses are delayed a litte bit
ENV["REMBUS_TIMEOUT"] = "60"

const MESSAGES = 100

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10, y=1:10)

mutable struct TestBag
    noarg_message_received::Bool
    msg_received::Int
end

function mytopic(bag::TestBag, rb, data::Number)
    @debug "mytopic recv: $data"
end

function mytopic(bag::TestBag, rb, data)
    @debug "df:\n$(view(data, 1:2, :))"
end

function publish_messages()
    pub = connect()
    for i in 1:MESSAGES
        publish(pub, "topicut")
    end
    sleep(2)
    close(pub)
end

topicut() = nothing

function read_messages()
    sub = connect("mysub")
    subscribe(sub, topicut, from=LastReceived())
    reactive(sub)
    sleep(5)
    close(sub)
end

function publish_macroapi(publisher, sub1; waittime=1)
    testbag = TestBag(false, 0)

    @component sub1
    @inject sub1 testbag
    @subscribe sub1 mytopic from = Now()
    @reactive sub1

    sleep(waittime / 3)

    @component publisher
    @publish publisher mytopic(2)
    @publish publisher mytopic(df)

    @publish publisher noarg()

    sleep(waittime / 2)

    for cli in [publisher, sub1]
        @shutdown cli
    end
end

function publish_api(pub, sub1; waittime=1)
    mytopic_topic = "mytopic_topic"
    noarg_topic = "noarg"

    testbag = TestBag(false, 0)

    publisher = connect(pub)

    sub1 = connect(sub1)
    inject(sub1, testbag)

    subscribe(sub1, mytopic_topic, mytopic)
    reactive(sub1)

    publish(publisher, mytopic_topic, 2)
    publish(publisher, mytopic_topic, df)
    publish(publisher, mytopic_topic, bdf)

    publish(publisher, noarg_topic)

    sleep(waittime)
    unsubscribe(sub1, mytopic_topic)

    sleep(waittime / 4)

    for cli in [publisher, sub1]
        close(cli)
    end
end

function add_one(add_one_arg)
    add_one_arg + 1
end

function request_api(request_url, exposer_url)
    rpc_topic = "rpc_method"
    request_arg = 1

    client = connect(request_url)

    try
        rpc(client, rpc_topic, raise=true)
    catch e
    end

    implementor = connect(exposer_url)
    expose(implementor, rpc_topic, add_one)

    res = rpc(client, rpc_topic, request_arg)

    for cli in [implementor, client]
        close(cli)
    end
end

mutable struct Holder
    valuemap::Vector
    Holder() = new([ # type of value received => value sent
        SmallInteger => SmallInteger(UInt(1)),
        SmallInteger => SmallInteger(1),
        SmallInteger => SmallInteger(-1),
        BigInt => BigInt(1),
        BigInt => BigInt(-1),
        UInt8 => Int8(1),
        UInt8 => UInt8(255),
        UInt16 => UInt16(256),
        UInt32 => UInt32(65536),
        UInt64 => UInt64(1),
        Int8 => Int8(-1),
        Int16 => Int16(-129),
        Int32 => Int32(-40000),
        Int64 => Int64(1),
        Float16 => Float16(1.0),
        Float32 => Float32(1.1),
        Float64 => Float64(1.2),
        DataFrame => DataFrame(:a => [1, 2]),
        String => "foo",
        Dict => Dict(1 => 2),
        Vector => [1, 2],
        Rembus.UndefLength => Rembus.UndefLength([1, 2]),
        Undefined => Undefined()])
end

function type_consumer(bag, rb, n)
    @debug "[type_consumer]: recv $n ($(typeof(n)))"
end

function types()
    bag = Holder()
    REQUESTOR = "tcp://:8001/type_publisher"
    TYPE_LISTENER = "type_listener"

    @component REQUESTOR
    @component TYPE_LISTENER

    sleep(0.1)

    @subscribe TYPE_LISTENER type_consumer from = Now()
    @inject TYPE_LISTENER bag
    @reactive TYPE_LISTENER

    sleep(0.1)
    for (typ, msg) in bag.valuemap
        sleep(0.05)
        @publish REQUESTOR type_consumer(msg)
    end
    sleep(2)
    @shutdown REQUESTOR
    @shutdown TYPE_LISTENER
end

function mymethod(n)
    return n + 1
end

mytopic(n) = nothing

function broker_server()
    server_url = "ws://:9005/s1"
    p = from("broker.broker")
    router = p.args[1]

    try
        srv = server(mode="anonymous", log="error", ws=9005)
        expose(srv, mymethod)
        subscribe(srv, mytopic)

        Rembus.islistening(wait=15, procs=["server.serve:9005"])

        add_node(router, server_url)
        sleep(2)

        cli = connect()
        rpc(cli, "mymethod", 1)
        publish(cli, "mytopic", 1)
        sleep(1)
        close(cli)
    catch e
        @error "broker_server: $e"
    finally
        remove_node(router, server_url)
    end
end

@rpc version()
@rpc uptime()
@shutdown

types()

request_api("requestor", "exposer")
waittime = 0.5
for sub1 in ["tcp://:8001/sub_tcp", "zmq://:8002/sub_zmq"]
    for publisher in ["tcp://:8001/pub", "zmq://:8002/pub"]
        publish_macroapi(publisher, sub1, waittime=waittime)
        publish_api(publisher, sub1, waittime=waittime)
    end
end

publish_messages()

broker_server()

response = HTTP.get("http://localhost:9000/version", [])

shutdown()
broker(wait=false, mode="anonymous", log="error")
yield()
Rembus.islistening(wait=20)
read_messages()
