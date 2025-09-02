using Rembus

mytopic(data) = println("[Sub-2] monitor inputs $data")

foo() = "sub-2.jl"

rb = component(name="sub-2", ws=3002)
subscribe(rb, mytopic)

expose(rb, foo)

println("up and running")
wait(rb)
