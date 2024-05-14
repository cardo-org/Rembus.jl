include("../utils.jl")

function run()
    try
        connect("myc")
    catch e
        @info "[test_authenticate_timeout] expected error: $e"
        @test isa(e, RembusTimeout)
    end
end

rembus_timeout = Rembus.request_timeout()
ENV["REMBUS_TIMEOUT"] = 0.001
execute(run, "test_authenticate_timeout")
ENV["REMBUS_TIMEOUT"] = rembus_timeout
