include("../utils.jl")

node = "authenticated_node"

function run()
    anonymous!()
    connect("named_only")
end

try
    execute(run, "test_auth_failed", mode="authenticated")
catch e
    @error "[test_auth_failed]: $e"
    @test false
finally
end
