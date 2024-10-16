include("../utils.jl")


function run()
    anonymous!()
    # connect without responding to the challenge
    rb = connect()
    # wait time necessary to close the connection because of challenge response timeout.
    sleep(Rembus.request_timeout() + 1)
    close(rb)
end

try
    execute(run, "test_challenge_not_sent", mode="authenticated")
    @test true
catch e
    @error "[test_challenge_not_sent]: $e"
    @test false
finally
end
