using Rembus
using Test

# Byte strings are base64 text (RFC 4648 §4, padded) in JSON.
@test Rembus.JSON3.write(Rembus.json_data(UInt8[1, 2, 3])) == "\"AQID\""
@test Rembus.JSON3.write(Rembus.json_data(UInt8[])) == "\"\""
@test Rembus.JSON3.write(Rembus.json_data(UInt8[1])) == "\"AQ==\""

# at any depth
@test Rembus.JSON3.write(Rembus.json_data(Dict("a" => UInt8[1, 2, 3]))) == "{\"a\":\"AQID\"}"
@test Rembus.JSON3.write(Rembus.json_data([UInt8[1, 2, 3], 1, "x"])) == "[\"AQID\",1,\"x\"]"

# integer arrays are ordinary arrays
@test Rembus.JSON3.write(Rembus.json_data([1, 2, 3])) == "[1,2,3]"
