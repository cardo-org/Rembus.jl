include("../utils.jl")

srv_topic(ctx, rb) = ctx["recv"] = true
srv_service(ctx, rb, val) = val + 2
cli_topic() = @info "[cli_topic] recv"
cli_service(val) = val * 2


function run()
    # a loopback condition throws an error
    @test_throws ErrorException component("my_cmp", ws=8000)

    ctx = Dict()
    srv = server(name="server", ws=8000)
    expose(srv, srv_service)
    subscribe(srv, srv_topic)
    inject(srv, ctx)

    cli = connect("server_client")
    expose(cli, cli_service)
    subscribe(cli, cli_topic)

    val = 3
    @test rpc(srv, "cli_service", val) == val * 2
    @test rpc(cli, "srv_service", val) == val + 2

    publish(cli, "srv_topic")
    publish(srv, "cli_topic")

    @test check_sentinel(ctx, sentinel="recv")

end

@info "[server] start"
try
    run()
catch e
    @test false
    @error "[server error: $e]"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[server] stop"
