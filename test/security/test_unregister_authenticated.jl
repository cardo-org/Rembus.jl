include("../utils.jl")

node = "authenticated_node"

function run()
    authenticated!()
    rb = connect(node)
    unregister(rb)
    close(rb)
end

try
    execute(run, "test_unregister_authenticated", mode="authenticated")
    @test true
catch e
    @error "[test_unregister_authenticated]: $e"
    @test false
finally
end
