include("../utils.jl")

# A server that request a method that returns a response that is an exception
# test layout:
# component - broker - server

function component_service()
    error("generic error")
end

function server_service(ctx, rb)
    rpc(rb, "component_service")
end

function run()
    server_url = "ws://:9000/myserver"
    srv = server(mode="anonymous", ws=9000)
    inject(srv)
    expose(srv, server_service)

    bro = broker(wait=false, name=BROKER_NAME)
    Rembus.islistening(wait=20, procs=["$BROKER_NAME.serve_ws"])
    add_node(bro, server_url)

    # it seems that coverage require some sleep time
    sleep(0.6)

    cli = connect()
    expose(cli, component_service)

    @test_throws RpcMethodException rpc(cli, "server_service")
    close(cli)

    cli = connect("ws://:9000/myclient")

    @test_throws Rembus.AlreadyConnected connect("ws://:9000/myclient")

    remove_node(bro, server_url)
end

@info "[test_server_error] start"
try
    run()
catch e
    @error "unexepected error: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
end
@info "[test_server_error] stop"
