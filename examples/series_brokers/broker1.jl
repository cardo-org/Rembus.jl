# 🟡 broker1.jl
using Rembus

rb = component("ws://localhost:3002/broker2", ws=3001)
wait(rb)
