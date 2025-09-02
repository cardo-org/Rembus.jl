# 🟢 publish.jl
using Rembus

rb = component(["ws://:3001/client", "ws://:3002/client"])
#rb = component("ws://:3001/client")

sensor1 = Dict("T" => 18.3, "H" => 45.2)
sensor2 = Dict("P" => 2.3)

publish(
    rb,
    "mytopic",
    Dict(
        "sensor#1" => sensor1,
        "sensor#2" => sensor2,
    )
)
