using Rembus
using Test

emb = embedded()

serve(emb, wait=false)
@test from("embedded") !== nothing

shutdown()

sleep(1)
@test from("caronte") === nothing
