include("../utils.jl")

function run()
    url = "ws://:9000/s1"
    srv = server()
    serve(srv, wait=false, args=Dict("ws" => 9000))
    router = caronte(wait=false, args=Dict("reset" => true))
    add_server(router, url)
    @test isa(from(url), Visor.Process)
    sleep(1)
    remove_server(router, url)

    shutdown()
end

@info "[test_broker_add_server] start"
run()
@info "[test_broker_add_server] stop"
