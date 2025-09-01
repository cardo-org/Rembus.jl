# 🟢 client.jl
using Rembus

rb = component(["ws://localhost:3001/cli", "ws://localhost:3002/cli"])

rpc(rb, "foo", 1.0)
