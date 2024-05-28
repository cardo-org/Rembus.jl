include("../utils.jl")

using DataFrames

mutable struct TestHolder
    handler_called::Int
    noarg_message_received::Bool
    valuemap::Vector
    TestHolder() = new(0, false, [ # type of value received => value sent
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
        Undefined => Undefined()
    ])
end

function foo(n)
    n + 1
end

function foo(bag, n)
    @debug "[foo]: recv $n" _group = :test
end

function bar(bag, n)
    @debug "[bar]: recv $n ($(typeof(n)))" _group = :test
    bag.handler_called += 1
    @atest isa(n, bag.valuemap[bag.handler_called].first) "$n isa $(bag.valuemap[bag.handler_called].first)"
end

function run()
    exposer = "test_types_exposer"
    requestor = "test_types_cli"
    listener = "test_types_listener"

    bag = TestHolder()

    @component exposer
    @component requestor
    @component listener

    sleep(0.1)

    @subscribe listener foo from_now
    @shared listener bag
    @reactive listener

    @expose exposer foo
    sleep(0.1)

    result = @rpc requestor foo(1)
    @debug "[$requestor] result=$result" _group = :test
    @test result == 2

    @subscribe exposer bar from_now
    @shared exposer bag
    @reactive exposer

    sleep(0.1)
    for (typ, msg) in bag.valuemap
        sleep(0.05)
        @publish requestor bar(msg)
    end

    sleep(5)
    @test bag.handler_called == length(bag.valuemap)
end

execute(run, "test_types")
