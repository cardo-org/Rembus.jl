include("../utils.jl")

function connection_failed()
    @component "probe"
    try
        @rpc version()
    catch e
        @info "[test_simple_rpc] expected error: $e"
        @test isa(e, ErrorException)
    finally
        @shutdown
    end
end

function run()
    @component "probe"
    try
        res = @rpc version()
        @test isa(res, String)
    catch e
        @test false
    finally
        @shutdown
    end
end

connection_failed()
execute(run, "test_simple_rpc", ws=8000)
