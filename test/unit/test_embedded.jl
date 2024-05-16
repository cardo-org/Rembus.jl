using Rembus
using Test

emb = server()

serve(emb, wait=false)
@test from("server") !== nothing

shutdown()

sleep(1)
@test from("caronte") === nothing
