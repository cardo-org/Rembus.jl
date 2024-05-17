using Rembus
using Test

# decode a simple value
buff = UInt8[0xe1]
@test decode(buff) == 1

n = Int32(1)
e = encode(n)
@test e == UInt8[0x1a, 0, 0, 0, 0x1]

n = Int64(1)
e = encode(n)
@test e == UInt8[0x1b, 0, 0, 0, 0, 0, 0, 0, 0x1]

n = Int64(-1)
e = encode(n)
@test e == UInt8[0x3b, 0, 0, 0, 0, 0, 0, 0, 0]

n = Int8(-1)
e = encode(n)
@test e == UInt8[0x20]

n128 = UInt128(1 << 62)^2
@test_throws ErrorException encode(Rembus.SmallInteger(n128))

@test typeof(decode(encode(SmallInteger(-1)))) == Int8
@test typeof(decode(encode(SmallInteger(-Int8(Rembus.INT8_MAX_POSITIVE))))) == Int8
@test typeof(decode(encode(SmallInteger(-(Rembus.INT8_MAX_POSITIVE + 2))))) == Int16
@test typeof(decode(encode(SmallInteger(-Int16(Rembus.INT16_MAX_POSITIVE))))) == Int16
@test typeof(decode(encode(SmallInteger(-(Rembus.INT16_MAX_POSITIVE + 2))))) == Int32
@test typeof(decode(encode(SmallInteger(-Int32(Rembus.INT32_MAX_POSITIVE))))) == Int32
@test typeof(decode(encode(SmallInteger(-(Rembus.INT32_MAX_POSITIVE + 2))))) == Int64
@test typeof(decode(encode(SmallInteger(-Int64(Rembus.INT64_MAX_POSITIVE))))) == Int64
@test typeof(decode(encode(SmallInteger(-Int128(Rembus.INT64_MAX_POSITIVE + 2))))) == Int128

@test convert(SmallInteger, 1) == 1
@test 1 == convert(SmallInteger, 1)
@test convert(SmallInteger, 1) == convert(SmallInteger, SmallInteger(1))

@test Rembus.Tag(1, 1) == Rembus.Tag(1, 1)
@test Rembus.num2hex(10) == "000000000000000a"
@test Rembus.hex2num(Rembus.num2hex(1.0)) == 1.0

v = Rembus.UndefLength([UInt8[1, 2], UInt8[3, 4]])
buff = encode(v)
decoded_v = decode(buff)
@test decoded_v == UInt8[1, 2, 3, 4]

d = Dict(
    "msg1" => 1,
    "msg2" => 2
)

function values(ch::Channel)
    for (k, v) in d
        put!(ch, k)
        put!(ch, v)
    end
end

buff = encode(Rembus.UndefLength{Pair}(Channel(values)))
res = decode(buff)
@test res == d

buff = encode(1.0)
# set an invalid value for type 7
buff[1] = Rembus.TYPE_7 | UInt8(28)
@test_throws ErrorException decode(buff)

buff = encode(UInt16(1))
buff[1] = Rembus.TYPE_0 | UInt8(28)
@test_throws ErrorException decode(buff)

buff = encode(Int16(-1))
buff[1] = Rembus.TYPE_1 | UInt8(28)
@test_throws ErrorException decode(buff)
