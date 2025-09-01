# 🔵 server.jl
using Rembus

foo(x) = 2x

rb = component(["ws://localhost:3001/broker1", "ws://localhost:3002/broker2"])
expose(rb, foo)

wait(rb)
