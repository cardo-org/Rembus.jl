include("../utils.jl")

function run()
    router = broker(wait=false, name=BROKER_NAME, reset=true)
    @test_throws Exception add_server(router, "bar")
    shutdown()
end

run()
