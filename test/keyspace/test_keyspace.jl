include("../utils.jl")

function environment(topic, value; ctx, node)
    ctx[topic] = value
end

function run()
    ctx = Dict()

    temperature = 18.5
    wind = 3

    bro = broker()

    sub1 = component("hierarchy_sub1")
    inject(sub1, ctx)
    subscribe(sub1, "veneto/*/cencenighe/*", environment)
    reactive(sub1)

    sub2 = component("hierarchy_sub2")
    inject(sub2, ctx)
    subscribe(sub2, "veneto/*/cencenighe/*", environment)
    reactive(sub2)

    node_name = "veneto/belluno/cencenighe"
    pub = component(node_name)
    put(pub, "temperature", temperature)
    put(pub, "wind", wind)

    # verbatim chunk is not matched by * regexp
    publish(pub, "veneto/@agordo/cencenighe/temperature", temperature)

    sleep(1)

    unsubscribe(sub2, "veneto/*/cencenighe/*")

    close(sub1)
    close(sub2)
    close(pub)
    close(bro)

    @test ctx[node_name*"/temperature"] == temperature
    @test ctx[node_name*"/wind"] == wind

    @test !haskey(ctx, "veneto/@agordo/cencenighe/temperature")
end

@info "[hierarchy] start"
try
    request_timeout!(5)
    run()
catch e
    @test false
    @error "[hierarchy] error: $e"
finally
    shutdown()
end
@info "[hierarchy] stop"
