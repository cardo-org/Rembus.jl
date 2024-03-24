#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

# When compiling responses are delayed a litte bit
ENV["REMBUS_TIMEOUT"] = "60"

df = DataFrame(name=["trento", "belluno"], score=[10, 50])
bdf = DataFrame(x=1:10, y=1:10)

mutable struct TestBag
    noarg_message_received::Bool
    msg_received::Int
end

function noarg()
end

function mytopic(bag::TestBag, data::Number)
    @debug "mytopic recv: $data"
end

function mytopic(bag::TestBag, data)
    @debug "df:\n$(view(data, 1:2, :))"
end

function publish_macroapi(publisher, sub1; waittime=1)
    testbag = TestBag(false, 0)

    @component sub1
    @shared sub1 testbag
    @subscribe sub1 mytopic from_now
    @reactive sub1

    sleep(waittime / 3)

    @component publisher
    @publish publisher mytopic(2)
    @publish publisher mytopic(df)

    @publish publisher noarg()

    sleep(waittime / 2)

    for cli in [publisher, sub1]
        @terminate cli
    end
end

function publish_api(pub, sub1; waittime=1)
    mytopic_topic = "mytopic_topic"
    noarg_topic = "noarg"

    testbag = TestBag(false, 0)

    publisher = connect(pub)

    sub1 = connect(sub1)
    shared(sub1, testbag)

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

function do_method_error(data)
    reason = "this is an error"
    error(reason)
end

function do_args_error(data)
    args_error_msg = "expected a float32"
    if !isa(data, Float32)
        throw(ErrorException(args_error_msg))
    end
end

function rpc_method(rpc_method_arg)
end

function request_api(request_url, exposer_url)
    rpc_topic = "rpc_method"
    request_arg = 1

    client = connect(request_url)

    try
        rpc(client, rpc_topic, exceptionerror=true)
    catch e
    end

    implementor = connect(exposer_url)
    expose(implementor, rpc_topic, add_one)

    res = rpc(client, rpc_topic, request_arg)

    expose(implementor, rpc_topic, do_method_error)
    try
        res = rpc(client, rpc_topic)
    catch e
    end

    for cli in [implementor, client]
        close(cli)
    end
end

mutable struct Holder
    valuemap::Vector
    Holder() = new([ # type of value received => value sent
        UInt8 => Int(1),
        UInt8 => 255,
        UInt16 => 256,
        UInt32 => 65536,
        Int8 => -1,
        Int16 => -129,
        Int32 => -40000,
        Float32 => Float32(1.2),
        Float64 => Float64(1.2),
        DataFrame => DataFrame(:a => [1, 2]),
        String => "foo",
        Dict => Dict(1 => 2),
        Vector => [1, 2]
    ])
end

function type_consumer(bag, n)
    @debug "[type_consumer]: recv $n ($(typeof(n)))"
end

function types()
    bag = Holder()
    REQUESTOR = "tcp://:8001/type_publisher"
    TYPE_LISTENER = "type_listener"

    @component REQUESTOR
    @component TYPE_LISTENER

    sleep(0.1)

    @subscribe TYPE_LISTENER type_consumer from_now
    @shared TYPE_LISTENER bag
    @reactive TYPE_LISTENER

    sleep(0.1)
    for (typ, msg) in bag.valuemap
        sleep(0.05)
        @publish REQUESTOR type_consumer(msg)
    end
    sleep(2)
    @terminate REQUESTOR
    @terminate TYPE_LISTENER
end

@rpc version()
@rpc uptime()
@terminate

try
    types()
catch e
    @error "precompile: $e"
    showerror(stdout, e, catch_backtrace())
end

#try
#    waittime = 0.5
#    for sub1 in ["tcp://:8001/sub_tcp", "zmq://:8002/sub_zmq"]
#        for publisher in ["tcp://:8001/pub", "zmq://:8002/pub"]
#            publish_macroapi(publisher, sub1, waittime=waittime)
#            publish_api(publisher, sub1, waittime=waittime)
#        end
#    end
#catch e
#    @error "precompile: $e"
#    showerror(stdout, e, catch_backtrace())
#end
