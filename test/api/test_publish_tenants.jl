include("../utils.jl")

function mytopic(data; ctx, node)
    @info "[$node] mytopic: $data"
    ctx[rid(node)] = data
end

function run()
    ctx = Dict()
    sub_org = connect("sub.org")
    inject(sub_org, ctx)
    subscribe(sub_org, mytopic)
    reactive(sub_org)

    sub_com = connect("sub.com")
    inject(sub_com, ctx)
    subscribe(sub_com, mytopic)
    reactive(sub_com)

    pub_org = connect("pub.org")
    publish(pub_org, "mytopic", "hello world")

    sleep(0.5)
    close(pub_org)
    close(sub_org)
    close(sub_com)

    @test ctx["sub.org"] == "hello world"
    @test haskey(ctx, "sub.com") == false
end

execute(run, "publish_tenants")
