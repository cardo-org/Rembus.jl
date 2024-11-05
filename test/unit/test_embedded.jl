using Rembus
using Test

server()
@test from("server") !== nothing

shutdown()

sleep(1)
@test from("broker") === nothing
