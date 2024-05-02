include("../utils.jl")

mutable struct TestCtx
    admin_count::UInt
    user_count::UInt
    TestCtx() = new(0, 0)
end


function admin_consume_all(ctx, topic, n)
    @info "[admin_consume_all] topic:$topic, value:$n"
    ctx.admin_count += 1
end

function consume_all(ctx, topic, n)
    @info "[consume_all] topic:$topic, value:$n"
    ctx.user_count += 1
end


function run(admin_component)
    ctx = TestCtx()
    sub = "mysub"
    producer = "myproducer"
    my_private_topic = "my_private_topic"

    user_sub = connect(sub)
    shared(user_sub, ctx)
    subscribe(user_sub, "*", consume_all)
    reactive(user_sub)

    admin_sub = connect(admin_component)
    shared(admin_sub, ctx)
    subscribe(admin_sub, "*", admin_consume_all)
    reactive(admin_sub)

    # define a private topic
    private_topic(admin_sub, my_private_topic)
    authorize(admin_sub, producer, my_private_topic)
    authorize(admin_sub, sub, my_private_topic)

    cli = connect(producer)
    publish(cli, "foo", 1)
    publish(cli, "bar", 2)
    publish(cli, my_private_topic, 3)

    sleep(0.5)
    close(cli)
    close(admin_sub)
    @test ctx.admin_count == 3
    @test ctx.user_count == 3
end

admin_component = "test_admin"

setup() = set_admin(admin_component)
execute(() -> run(admin_component), "test_subscribe_glob", setup=setup)
rm(Rembus.root_dir(), recursive=true)
