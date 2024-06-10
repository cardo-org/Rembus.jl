include("../utils.jl")
using HTTP

# tests: 6

function run()
    rb = connect()
    msg = Rembus.IdentityMsg("")
    response = Rembus.wait_response(rb, msg, 2)
    @test response.status == Rembus.STS_GENERIC_ERROR
    close(rb)
end

execute(run, "test_identity_empty_string")
