include("../utils.jl")

srv_topic(; ctx, node) = ctx["recv"] = true
srv_service(val; ctx, node) = val + 2
cli_topic() = @info "[cli_topic] recv"
cli_service(val) = val * 2
other_topic() = (OTHER_RECV[] = true)
const OTHER_RECV = Ref(false)
srv_other_topic(; ctx, node) = ctx["other"] = true


function run()
    # a loopback condition throws an error
    @test_throws ErrorException component("my_cmp", ws=8338)

    ctx = Dict()
    srv = server(name="server", ws=8338)
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

    # a publish from a client is not relayed to the other clients
    other = connect("server_other")
    subscribe(other, other_topic); reactive(other)
    subscribe(srv, srv_other_topic)
    sleep(0.5)
    publish(cli, "other_topic")
    publish(cli, "srv_other_topic")
    @test check_sentinel(ctx, sentinel="other")
    sleep(1)
    @test !OTHER_RECV[]

    # a publish from the server reaches its subscribers
    publish(srv, "other_topic")
    sleep(1)
    @test OTHER_RECV[]

    # direct is not permitted when the component is a server
    try
        direct(cli, "server_client", "cli_service", val)
        @test false
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code == Rembus.STS_GENERIC_ERROR
        @test e.reason == "direct not permitted in server mode"
    end

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
