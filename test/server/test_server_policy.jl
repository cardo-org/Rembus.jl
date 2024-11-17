include("../utils.jl")

function service1()
    # simulate a slow response for lessbusy policy test
    sleep(0.5)
    return 1
end

service2() = 2

function start_server1()
    rb = server(ws=8000, tcp=8001, log="info")
    expose(rb, "myservice", service1)
    forever(rb)
end

function start_server2()
    rb = server(ws=9000, tcp=9001, log="info")
    expose(rb, "myservice", service2)
    forever(rb)
end

function run()

    Rembus.islistening(wait=10, procs=[
        "server.serve:8000",
        "server.serve:9000",
        "server.serve_tcp:8001",
        "server.serve_tcp:9001"
    ])

    client = component(["ws://:8000/c1", "ws://:9000/c2"])
    sleep(1)
    Visor.dump()
    # set broker policy to roundrobin
    roundrobin_policy(client)

    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response"
    @test response == 1

    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response"
    @test response == 2

    # set broker policy to lessbusy
    lessbusy_policy(client)

    # send a request and return immediately, so that the next
    # request get delivered to server2 because this request has to be
    # served by server1
    @async begin
        try
            @info "first lessbusy request"
            response = rpc(client, "myservice")
            @info "[test_server_policy] myservice()=$response (expected 1)"
            @test response == 1
        catch e
            @error "[test_server_policy] error: $e"
        end
    end
    sleep(0.1)
    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response (expected 2)"
    @test response == 2

    sleep(1)

    firstup_policy(client)
    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response"
    @test response == 1

    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response"
    @test response == 1

    all_policy(client)
    response = rpc(client, "myservice")
    @info "[test_server_policy] myservice()=$response"
    @test response == [1, 2]

    terminate(client)
end

@info "[test_server_policy] start"
try
    @async start_server1()
    @async start_server2()
    run()
catch e
    @test false
finally
    shutdown()
end

@info "[test_server_policy] stop"
