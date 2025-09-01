# 🔵 server.jl
using Rembus

foo(x) = 2x

rb = component("ws://localhost:3002/srv")
expose(rb, foo)

wait(rb)
