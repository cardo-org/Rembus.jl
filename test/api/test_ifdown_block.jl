include("../utils.jl")

srv_topic(; ctx, node) = ctx["recv"] = true
srv_service(val; ctx, node) = val + 2
cli_topic() = @info "[cli_topic] recv"
cli_service(val) = val * 2


function start_delayed(ctx)
    sleep(2)

    bro = broker(name="ifdown_block", ws=8000)
    inject(bro, ctx)
    expose(bro, srv_service)
    subscribe(bro, srv_topic)
end

function run()
    ctx = Dict()
    cli = component("ifdown_block_client")
    Rembus.ifdown_block(cli)

    @async start_delayed(ctx)

    val = 3
    @test rpc(cli, "srv_service", val) == val + 2

    publish(cli, "srv_topic")

    @test check_sentinel(ctx, sentinel="recv")

end

@info "[ifdown_block] start"
try
    run()
catch e
    @test false
    @error "[ifdown_block error: $e]"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[ifdown_block] stop"
