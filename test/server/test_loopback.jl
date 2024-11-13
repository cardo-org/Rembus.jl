include("../utils.jl")

function run()
    router = broker(wait=false, name=BROKER_NAME, reset=true)
    @test_throws Exception add_node(router, "bar")
    shutdown()
end

run()
