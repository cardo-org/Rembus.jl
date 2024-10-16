include("../utils.jl")


function run()
    authenticated!()
    rb = connect("named")
    close(rb)
end

try
    execute(run, "test_inquiry_not_sent", mode="anonymous")
    @test true
catch e
    @error "[test_inquiry_not_sent]: $e"
    @test false
finally
end
