using Rembus
using Test

caronte(wait=false)
@test from("caronte") !== nothing
shutdown()

sleep(1)
@test from("caronte") === nothing
