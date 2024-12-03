include("../utils.jl")

try
    broker(wait=false)
    @test from("broker") !== nothing
catch e
    @error "[test_caronte] unexpected error: $e"
finally
    shutdown()
end

sleep(1)
@test from("broker") === nothing

# do nothing
@test Visor.shutdown(nothing) === nothing
