include("../utils.jl")

function run()
    url = "ws://127.0.0.1:9000/s1"
    server(ws=9000)
    router = broker(wait=false, name=BROKER_NAME, reset=true)
    add_node(router, url)
    @test isa(from_name(url), Visor.Process)
    sleep(1)
    remove_node(router, url)

    shutdown()
end

@info "[test_broker_add_server] start"
run()
@info "[test_broker_add_server] stop"
