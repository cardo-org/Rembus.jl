include("../utils.jl")

caronte(wait=false)
@test from("caronte") !== nothing
shutdown()

sleep(1)
@test from("caronte") === nothing

# do nothing
@test Visor.shutdown(nothing) === nothing
