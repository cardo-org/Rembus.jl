using DataFrames
using Rembus
using Test

df = DataFrame(x=1:100)
topic = "mytopic"
msg = Rembus.PubSubMsg(topic, df)
msginfo = Rembus.prettystr(msg)
@test msginfo == topic

msg = Rembus.PubSubMsg(topic, zeros(UInt8, 100))
msginfo = Rembus.prettystr(msg)
@test startswith(msginfo, "binary[100] UInt8[0x00,")
