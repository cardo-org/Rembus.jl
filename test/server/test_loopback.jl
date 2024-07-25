include("../utils.jl")

function run()
    router = caronte(wait=false, args=Dict("reset" => true))
    @test_throws Exception add_server(router, "bar")
    shutdown()
end

run()
