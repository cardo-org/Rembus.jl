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
