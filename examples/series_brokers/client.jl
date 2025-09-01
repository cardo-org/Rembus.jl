# 🟢 client.jl
using Rembus

rb = component("ws://localhost:3001/cli")
response = rpc(rb, "foo", 12.0)

close(rb)
