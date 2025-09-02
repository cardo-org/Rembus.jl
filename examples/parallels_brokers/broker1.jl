# 🟡 broker1.jl
using Rembus

rb = component(ws=3001)

println("up and running")
wait(rb)
