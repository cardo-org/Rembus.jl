include("../utils.jl")

mutable struct TestCtx
    admin_count::UInt
    user_count::UInt
    TestCtx() = new(0, 0)
end


function admin_consume_all(topic, n, m=1; ctx, node)
    @info "[admin_consume_all] topic:$topic, n=$n, m=$m"
    ctx.admin_count += 1
end

function consume_all(topic, n, m=1; ctx, node)
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
    sub = "subscribe_glob_sub"
    subzmq = "zmq://:8002/subscribe_glob_subzmq"
    sub_noshared = "subscribe_glob_another_sub"

    producer = "subscribe_glob_producer"
    producerzmq = "zmq://:8002/subscribe_glob_producerzmq"
    my_private_topic = "my_private_topic"

    user_sub = connect(sub)
    inject(user_sub, ctx)
    subscribe(user_sub, "*", consume_all)
    reactive(user_sub)

    user_subzmq = connect(subzmq)
    subscribe(user_subzmq, "*", another_consume)
    reactive(user_subzmq)

    another_sub = connect(sub_noshared)
    subscribe(another_sub, "*", another_consume)
    reactive(another_sub)

    admin_sub = connect(admin_component)
    inject(admin_sub, ctx)
    subscribe(admin_sub, "*", admin_consume_all)
    reactive(admin_sub)

    # define a private topic
    private_topic(admin_sub, my_private_topic)
    authorize(admin_sub, producer, my_private_topic)
    authorize(admin_sub, sub, my_private_topic)

    cli = connect(producer)
    publish(cli, "foo", 1)
    publish(cli, "bar", 2)
    publish(cli, "bar", 1, 2)
    publish(cli, my_private_topic, 3)

    clizmq = connect(producerzmq)
    publish(clizmq, "foo", 1)

    # generate a broker error log
    publish(cli, "bar", 1, 2, 3)

    sleep(1)
    for c in [cli, clizmq, admin_sub, user_sub, user_subzmq]
        close(c)
    end
    @test ctx.admin_count == 9
    @test ctx.user_count == 13
end

admin_component = "subscribe_glob_admin"

broker_name = "subscribe_glob"
setup() = set_admin(broker_name, admin_component)
execute(() -> run(admin_component), broker_name, setup=setup)
