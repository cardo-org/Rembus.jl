include("../utils.jl")

function service1()
    # simulate a slow response for lessbusy policy test
    sleep(0.5)
    return 1
end

service2() = 2

function run()

    Visor.dump()

    router = Rembus.get_router(BROKER_NAME)
    @info "router:$router"

    s1 = connect("s1")
    expose(s1, "myservice", service1)

    s2 = connect("s2")
    expose(s2, "myservice", service2)

    client = connect("client")

    # set broker policy to roundrobin
    roundrobin_policy(router)

    response = rpc(client, "myservice")
    @info "[test_policy] myservice()=$response"
    @test response == 1

    response = rpc(client, "myservice")
    @info "[test_policy] myservice()=$response"
    @test response == 2

    # set broker policy to lessbusy
    lessbusy_policy(router)

    # send a request and return immediately, so that the next
    # request get delivered to server2 because this request has to be
    # served by server1
    @async begin
        try
            @info "first lessbusy request"
            response = rpc(client, "myservice")
            @info "[test_policy] myservice()=$response (expected 1)"
            @test response == 1
        catch e
            @error "[test_policy] error: $e"
        end
    end
    sleep(0.1)
    response = rpc(client, "myservice")
    @info "[test_policy] myservice()=$response (expected 2)"
    @test response == 2

    sleep(1)

    firstup_policy(router)
    response = rpc(client, "myservice")
    @info "[test_policy] myservice()=$response"
    @test response == 1

    response = rpc(client, "myservice")
    @info "[test_policy] myservice()=$response"
    @test response == 1

    close(s1)
    close(s2)
    close(client)
end

execute(run, "test_policy")
