include("../utils.jl")

mutable struct TestHolder
    msg_received::Int
    TestHolder() = new(0)
end

function consume(bag, rb, data)
    bag.msg_received += 1
end

function private_service(bag, rb, n)
    return n + 1
end

another_service() = nothing

another_topic() = nothing

function run(authorized_component)
    bag = TestHolder()

    priv_topic = "foo"
    priv_service = "private_service"
    another_priv_topic = "bar"
    myproducer = "private_topic_myproducer"
    myconsumer = "private_topic_myconsumer"
    myunauth = "private_topic_myunauth"

    rb = connect(authorized_component)

    res = authorize(rb, myconsumer, priv_topic)
    @test isnothing(res)

    private_topic(rb, priv_topic)
    private_topic(rb, another_priv_topic)
    private_topic(rb, priv_service)

    authorize(rb, myproducer, priv_topic)

    authorize(rb, myconsumer, priv_service)

    producer = connect(myproducer)

    cfg = get_private_topics(rb)
    @test_throws RembusError get_private_topics(producer)

    sleep(0.1)
    try
        # authenticating again is considered a protocol error
        # the connection get closed
        Rembus.authenticate(producer.router, producer)
        @test false
    catch e
        @info "[$producer] expected error: $e"
    end

    producer = connect(myproducer)
    # producer is not an admin
    @test_throws RembusError authorize(producer, myproducer, another_priv_topic)
    @test_throws RembusError unauthorize(producer, myproducer, another_priv_topic)

    try
        private_topic(producer, another_priv_topic)
    catch e
        @info "[test_private_topic] expected error: $e"
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    unauth_consumer = connect(myunauth)
    consumer = connect(myconsumer)
    for c in [unauth_consumer, consumer]
        inject(c, bag)
    end

    @info "[test_private_topic] subscribe with no authorization"
    try
        subscribe(unauth_consumer, priv_topic, consume)
        reactive(unauth_consumer)
        @test false
    catch e
        @info "[test_private_topic] expected error: $e"
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    @test_throws RembusError unsubscribe(unauth_consumer, priv_topic)

    @test_throws RembusError expose(unauth_consumer, priv_topic, consume)

    subscribe(consumer, priv_topic, consume)
    expose(consumer, private_service)
    expose(rb, another_service)
    subscribe(rb, another_topic)

    reactive(consumer)

    publish(producer, priv_topic, "some_data")
    sleep(1)

    # unknow topic
    @test_throws RembusError unsubscribe(consumer, "unknown_topic")
    @test_throws RembusError unexpose(consumer, "unknown_topic")

    # component not authorized to topic
    @test_throws RembusError unexpose(consumer, another_priv_topic)

    # topic not exposed by component
    @test_throws RembusError unexpose(consumer, another_service)
    @test_throws RembusError unsubscribe(consumer, another_topic)

    unexpose(consumer, priv_service)

    for cmd in [
        Rembus.BROKER_CONFIG_CMD,
        Rembus.LOAD_CONFIG_CMD,
        Rembus.SAVE_CONFIG_CMD,
        Rembus.SAVE_CONFIG_CMD,
        Rembus.ENABLE_DEBUG_CMD,
        Rembus.DISABLE_DEBUG_CMD,
    ]
        Rembus.admin(rb, Dict(Rembus.COMMAND => cmd))
    end

    @test_throws RembusError Rembus.admin(rb, Dict(Rembus.COMMAND => "unknown command"))

    unauthorize(rb, myproducer, priv_topic)

    public_topic(rb, priv_topic)
    subscribe(unauth_consumer, priv_topic, consume)
    unsubscribe(unauth_consumer, priv_topic)

    # producer is not authorized
    try
        rpc(producer, "private_service", 1)
    catch e
        @debug "[RPC] expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    publish(producer, another_priv_topic, "some_data")

    try
        public_topic(producer, "some_topic")
    catch e
        @debug "expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    @test_throws RembusError Rembus.broker_shutdown(producer)

    for cmd in [
        Rembus.BROKER_CONFIG_CMD,
        Rembus.LOAD_CONFIG_CMD,
        Rembus.SAVE_CONFIG_CMD,
        Rembus.SAVE_CONFIG_CMD,
        Rembus.ENABLE_DEBUG_CMD,
        Rembus.DISABLE_DEBUG_CMD,
    ]
        @test_throws RembusError Rembus.admin(
            producer, Dict(Rembus.COMMAND => cmd)
        )
    end

    # execute a shutdown
    Rembus.broker_shutdown(rb)
    Visor.dump()

    for c in [rb, producer, consumer, unauth_consumer]
        shutdown(c)
    end

    @test bag.msg_received === 1
end

broker_name = "private_topic"
authorized_component = "private_topic_component"

setup() = set_admin(broker_name, authorized_component)

execute(() -> run(authorized_component), broker_name, setup=setup)
