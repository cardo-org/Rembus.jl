#using JSON3

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

function another_service()
end

function run(authorized_component)
    bag = TestHolder()

    priv_topic = "foo"
    priv_service = "private_service"
    another_priv_topic = "bar"
    myproducer = "myproducer"
    myconsumer = "myconsumer"
    myunauth = "myunauth"

    rb = tryconnect(authorized_component)

    authorize(rb, myconsumer, priv_topic)

    private_topic(rb, priv_topic)
    private_topic(rb, another_priv_topic)
    private_topic(rb, priv_service)

    authorize(rb, myproducer, priv_topic)

    authorize(rb, myconsumer, priv_service)

    producer = connect(myproducer)

    sleep(0.1)
    try
        # authenticating again is considered a protocol error
        # the connection get closed
        Rembus.authenticate(producer)
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
        @debug "expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    unauth_consumer = connect(myunauth)
    consumer = connect(myconsumer)
    for c in [unauth_consumer, consumer]
        inject(c, bag)
    end

    try
        subscribe(unauth_consumer, priv_topic, consume)
        reactive(unauth_consumer)
    catch e
        @debug "expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    @test_throws RembusError unsubscribe(unauth_consumer, priv_topic)
    @test_throws RembusError expose(unauth_consumer, priv_topic, consume)

    subscribe(consumer, priv_topic, consume)
    expose(consumer, private_service)
    expose(rb, another_service)

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

    unexpose(consumer, priv_service)

    unauthorize(rb, myproducer, priv_topic)

    public_topic(rb, priv_topic)
    subscribe(unauth_consumer, priv_topic, consume)

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

    # execute a shutdown
    @info "shutdown request"
    Rembus.broker_shutdown(rb)
    sleep(2)

    for c in [rb, producer, consumer, unauth_consumer]
        close(c)
    end
    @test bag.msg_received === 1
end

authorized_component = "test_private"

setup() = set_admin(authorized_component)

execute(() -> run(authorized_component), "test_private_topic", setup=setup)

rm(Rembus.broker_dir(BROKER_NAME), recursive=true, force=true)
