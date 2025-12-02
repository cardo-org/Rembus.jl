include("../utils.jl")

function mytopic(msg; ctx, node)
    @info "[mytopic] received message: $msg"
    ctx["msg"] = msg
    @info "[mytopic] context updated: $ctx"
end


function run()
    ctx = Dict{String,Any}()

    x = 10
    y = 20
    cli = component("jsonrpc_client", enc=Rembus.JSON)

    sub = component("jsonrpc_subscriber", enc=Rembus.JSON)
    subscribe(sub, mytopic)
    inject(sub, ctx)
    reactive(sub)

    msg = "hello world"
    publish(cli, "mytopic", msg, qos=Rembus.QOS2)

    sleep(2) # wait for JIT
    @test ctx["msg"] == msg

    close(cli)
    close(sub)
end

execute(run, "test_jsonrpc_pubsub")
