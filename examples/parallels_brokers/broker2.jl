# 🟡 broker2.jl
using Rembus

rb = component(ws=3002)

println("up and running")
wait(rb)
