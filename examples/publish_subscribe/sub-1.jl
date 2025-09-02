using Rembus

mytopic(data) = println("[Sub-1] updating DB with $data")

foo() = "sub-1.jl"

rb = component(name="sub-1", ws=3001)
subscribe(rb, mytopic)

expose(rb, foo)

println("up and running")
wait(rb)
