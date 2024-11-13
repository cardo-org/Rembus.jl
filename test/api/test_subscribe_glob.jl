include("../utils.jl")

mutable struct TestCtx
    admin_count::UInt
    user_count::UInt
    TestCtx() = new(0, 0)
end


function admin_consume_all(ctx, rb, topic, n, m=1)
    @info "[admin_consume_all] topic:$topic, n=$n, m=$m"
    ctx.admin_count += 1
end

function consume_all(ctx, rb, topic, n, m=1)
    @info "[consume_all] topic:$topic, n=$n, m=$m"
    ctx.user_count += 1
end

function another_consume(topic, x)
    @info "[another_consume] $topic: x=$x"
end

function another_consume(topic, x, y)
    @info "[another_consume] $topic: x=$x, y=$y"
end

function run(admin_component)
    ctx = TestCtx()
    sub = "mysub"
    sub_noshared = "another_sub"

    producer = "myproducer"
    my_private_topic = "my_private_topic"

    user_sub = connect(sub)
    shared(user_sub, ctx)
    subscribe(user_sub, "*", consume_all)
    reactive(user_sub)

    another_sub = connect(sub_noshared)
    subscribe(another_sub, "*", another_consume)
    reactive(another_sub)

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
    publish(cli, "bar", [1, 2])
    publish(cli, my_private_topic, 3)

    # generate a broker error log
    publish(cli, "bar", [1, 2, 3])

    sleep(0.5)
    close(cli)
    close(admin_sub)
    @test ctx.admin_count == 4
    @test ctx.user_count == 4
end

admin_component = "test_admin"

setup() = set_admin(admin_component)
execute(() -> run(admin_component), "test_subscribe_glob", setup=setup)
rm(Rembus.broker_dir(BROKER_NAME), recursive=true)
