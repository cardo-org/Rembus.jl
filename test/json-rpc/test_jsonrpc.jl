include("../utils.jl")

function myservice(x, y)
    return x + y
end

function mytopic(ctx, rb, msg)
    @info "[mytopic] received message: $msg"
    ctx["msg"] = msg
    @info "[mytopic] context updated: $ctx"
end


function run()
    ctx = Dict{String,Any}()

    srv = component("jsonrpc_server", enc=Rembus.JSON)
    expose(srv, myservice)

    x = 10
    y = 20
    cli = component("jsonrpc_client", enc=Rembus.JSON)
    result = rpc(cli, "myservice", x, y)
    @info "result=$result"
    @test result == x + y

    sub = component("jsonrpc_subscriber", enc=Rembus.JSON)
    subscribe(sub, mytopic)
    inject(sub, ctx)
    reactive(sub)

    msg = "hello world"
    publish(cli, "mytopic", msg)

    sleep(2) # wait for JIT
    @test ctx["msg"] == msg

    close(cli)
    close(sub)
    close(srv)
end

execute(run, "test_jsonrpc")
