include("../utils.jl")

function myservice(ctx, rb, data)
    @info "[$rb] myservice: $data"
    return data + 1
end

function run()
    ctx = Dict()
    org = connect("exposer.org")
    inject(org, ctx)
    expose(org, myservice)

    com = connect("exposer.com")
    inject(com, ctx)

    cli_org = connect("rpc_client.org")
    @test rpc(cli_org, "myservice", 1) == 2

    cli_com = connect("rpc_client.com")
    @test_throws Rembus.RpcMethodNotFound rpc(cli_com, "myservice", 1)

    close(cli_org)
    close(org)
    close(com)

end

execute(run, "rpc_tenants")
