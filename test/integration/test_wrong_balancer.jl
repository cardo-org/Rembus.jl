include("../utils.jl")

ENV["REMBUS_BALANCER"] = "wrong_balancer"

tmr = Timer((tmr) -> shutdown(), 5)
try
    Rembus.broker(wait=true)
    @test false
catch e
    @test e.msg === "wrong balancer, must be one of all, first_up, less_busy, round_robin"
finally
    close(tmr)
    shutdown()
end

ENV["REMBUS_BALANCER"] = "first_up"
