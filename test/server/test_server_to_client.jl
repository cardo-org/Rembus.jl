include("../utils.jl")

function rpc_service(x, y)
    return x + y
end

function run()
    try
        srv = server(ws=8000, log="info")
        is_up = islistening(srv, wait=10)
        @test is_up === true

        rb = connect("mycomponent")

        # the client expose and the serve makes a rpc request
        expose(rb, rpc_service)

        lessbusy_policy(srv)
        result = rpc(srv, "rpc_service", [1, 2])
        @test result == 3
    finally
        shutdown()
    end

end

@info "[test_server_to_client] start"
try
    run()
catch e
    @error "[test_server_to_client] error: $e"
    @test false
end
@info "[test_server_to_client] stop"
