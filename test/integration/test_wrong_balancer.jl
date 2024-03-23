include("../utils.jl")

ENV["BROKER_BALANCER"] = "wrong_balancer"

tmr = Timer((tmr) -> shutdown(), 5)
try
    Rembus.caronte(wait=true, exit_when_done=false)
    @test false
catch e
    @test e.msg === "wrong balancer, must be one of first_up, less_busy, round_robin"
finally
    close(tmr)
end

ENV["BROKER_BALANCER"] = "first_up"
