include("../utils.jl")

using DataFrames

mutable struct TestHolder
    handler_called::Int
    noarg_message_received::Bool
    valuemap::Vector
    TestHolder() = new(0, false, [ # type of value received => value sent
        UInt8 => Rembus.SmallInteger(UInt(1)),
        UInt8 => Rembus.SmallInteger(2),
        Int8 => Rembus.SmallInteger(-3),
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
        Int64 => Int64(-1),
        Float16 => Float16(1.0),
        Float32 => Float32(1.1),
        Float64 => Float64(1.2),
        DataFrame => DataFrame(:a => [1, 2]),
        String => "foo",
        Dict => Dict(1 => 2),
        Vector => [[1, 2]],
        Vector => [Rembus.UndefLength([1, 2])],
        Undefined => Undefined()
    ])
end

function bar(n; ctx, node)
    return n
end

function run(client, server)
    bag = TestHolder()

    exposer = component(server)
    requestor = component(client)

    inject(exposer, bag)
    expose(exposer, bar)

    for (typ, msg) in bag.valuemap
        sleep(0.05)
        result = rpc(requestor, "bar", msg)
        bag.handler_called += 1
        @test isa(result, bag.valuemap[bag.handler_called].first)
    end

    @test bag.handler_called == length(bag.valuemap)
end

client = "test_types_cli"
server = "test_types_exposer"
execute(() -> run(client, server), "types")
