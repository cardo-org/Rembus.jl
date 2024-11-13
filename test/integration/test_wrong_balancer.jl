include("../utils.jl")

tmr = Timer((tmr) -> shutdown(), 5)
try
    Rembus.broker(wait=true, policy=:wrong_balancer)
    @test false
catch e
    @test e.msg === "wrong broker policy, must be one of :first_up, :less_busy, :round_robin"
finally
    close(tmr)
    shutdown()
end
