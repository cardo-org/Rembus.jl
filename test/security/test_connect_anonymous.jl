include("../utils.jl")

node = "authenticated_node"

function run()
    anonymous!()
    rb = connect(node)
    sleep(1)
    close(rb)
end

try
    execute(run, "test_connect_anonymous", mode="authenticated")
    @test true
catch e
    @error "[test_connect_anonymous]: $e"
    @test false
finally
end
