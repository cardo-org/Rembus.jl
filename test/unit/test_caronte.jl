include("../utils.jl")

broker(wait=false)
@test from("broker") !== nothing
shutdown()

sleep(1)
@test from("broker") === nothing

# do nothing
@test Visor.shutdown(nothing) === nothing
