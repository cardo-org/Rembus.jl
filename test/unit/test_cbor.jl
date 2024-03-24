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
