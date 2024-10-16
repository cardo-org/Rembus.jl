include("../utils.jl")

node = "authenticated_node"

function run()
    anonymous!()
    rb = connect()
    @test_throws RembusTimeout rpc(rb, "version")
    close(rb)
end

try
    execute(run, "test_unauth_command", mode="authenticated")
catch e
    @error "[test_unauth_command]: $e"
    @test false
finally
end
