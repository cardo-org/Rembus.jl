include("../utils.jl")

mutable struct Counter
    n::Int
    Counter() = new(0)
end

consume(data; ctx, node) = ctx.n += 1

glob_consume(topic, data; ctx, node) = ctx.n += 1

# Wait for a delivery (the first one includes compilation latency).
wait_for(cond; timeout=20) = timedwait(cond, timeout) === :ok

function run(admin_name)
    # wildcard subscriptions need the keyspace extension
    Rembus.add_plugin(Visor.from("msgdel.broker").args[1], Rembus.KeySpaceRouter())

    admin = connect(admin_name)
    private_topic(admin, "secret/t")
    authorize(admin, "msgdel_auth", "secret/t")
    authorize(admin, "msgdel_pub", "secret/t")

    auth = connect("msgdel_auth")
    unauth = connect("msgdel_unauth")
    lazy = connect("msgdel_lazy")
    pub = connect("msgdel_pub")

    counters = Dict(n => Counter() for n in [:auth, :unauth, :lazy, :pub])
    inject(auth, counters[:auth])
    inject(unauth, counters[:unauth])
    inject(lazy, counters[:lazy])
    inject(pub, counters[:pub])

    # A wildcard subscription does not grant access to a private topic.
    subscribe(auth, "secret/*", glob_consume)
    subscribe(unauth, "secret/*", glob_consume)
    reactive(auth)
    reactive(unauth)

    publish(pub, "secret/t", 1)
    @test wait_for(() -> counters[:auth].n == 1)
    sleep(0.5)
    @test counters[:unauth].n == 0

    # A non reactive wildcard subscriber and the publisher itself get nothing.
    subscribe(lazy, "pub/*", glob_consume)
    subscribe(pub, "pub/*", glob_consume)
    subscribe(auth, "pub/*", glob_consume)
    reactive(pub)
    publish(pub, "pub/x", 1)
    @test wait_for(() -> counters[:auth].n == 2)
    sleep(0.5)
    @test counters[:lazy].n == 0
    @test counters[:pub].n == 0
    @test counters[:auth].n == 2

    # Authorization is evaluated at delivery time for exact topics.
    exact = Counter()
    exact_sub = connect("msgdel_exact")
    authorize(admin, "msgdel_exact", "secret/t")
    inject(exact_sub, exact)
    subscribe(exact_sub, "secret/t", consume)
    reactive(exact_sub)
    publish(pub, "secret/t", 2)
    @test wait_for(() -> exact.n == 1)
    unauthorize(admin, "msgdel_exact", "secret/t")
    publish(pub, "secret/t", 3)
    sleep(0.5)
    @test exact.n == 1

    # Publishes with an invalid topic are dropped.
    before = counters[:auth].n
    publish(pub, "", 1)
    publish(pub, "__reserved__", 1)
    publish(pub, "pub/*", 1)
    sleep(0.5)
    @test counters[:auth].n == before

    for c in [admin, auth, unauth, lazy, pub, exact_sub]
        shutdown(c)
    end
end

broker_name = "msgdel"
admin_name = "msgdel_admin"

setup() = set_admin(broker_name, admin_name)

execute(() -> run(admin_name), broker_name, setup=setup)
