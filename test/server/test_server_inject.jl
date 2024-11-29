include("../utils.jl")

function service(ctx, rb, val)
    latest_service(ctx, rb) = 2
    expose(rb.router, latest_service)

    ctx["service"] = val
    return val
end


function start_server()
    ctx = Dict()
    rb = server(ws=8000, tcp=8001, log="info")
    expose(rb, service)
    inject(rb, ctx)
    forever(rb)
end

function run()

    Rembus.islistening(wait=10, procs=[
        "server.serve:8000",
        "server.serve_tcp:8001",
    ])

    client = component("ws://:8000/c1")
    sleep(1)

    response = rpc(client, "service", [1])
    @info "[test_server_inject] service()=$response"
    @test response == 1

    response = rpc(client, "latest_service")
    @info "[test_server_inject] latest_service()=$response"
    @test response == 2

    shutdown(client)
end

@info "[test_server_inject] start"
try
    @async start_server()
    run()
catch e
    @test false
finally
    shutdown()
end

@info "[test_server_inject] stop"
