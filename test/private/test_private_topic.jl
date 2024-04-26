using JSON3

include("../utils.jl")

mutable struct TestHolder
    msg_received::Int
    TestHolder() = new(0)
end

function consume(bag, data)
    bag.msg_received += 1
end

function private_service(bag, n)
    return n + 1
end

function setup(name)
    if !isdir(Rembus.root_dir())
        mkdir(Rembus.root_dir())
    end

    # add admin privilege to client with name equals to test_private
    fn = joinpath(Rembus.root_dir(), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([name])))
    end
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

    private_topic(rb, priv_topic)
    private_topic(rb, priv_service)

    authorize(rb, myproducer, priv_topic)

    authorize(rb, myproducer, another_priv_topic)
    authorize(rb, myconsumer, priv_topic)
    authorize(rb, myconsumer, priv_service)

    producer = connect(myproducer)

    # producer is not an admin
    #    try
    #        authorize(producer, myproducer, another_priv_topic)
    #    catch e
    #        @debug "expected error: $e" _group = :test
    #        @test isa(e, Rembus.RembusError)
    #        @test e.code === Rembus.STS_GENERIC_ERROR
    #    end
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
        shared(c, bag)
    end

    try
        subscribe(unauth_consumer, priv_topic, consume)
        reactive(unauth_consumer)
    catch e
        @debug "expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    subscribe(consumer, priv_topic, consume)
    expose(consumer, private_service)
    reactive(consumer)

    publish(producer, priv_topic, "some_data")
    sleep(0.2)

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

    try
        public_topic(producer, "some_topic")
    catch e
        @debug "expected error: $e" _group = :test
        @test isa(e, Rembus.RembusError)
        @test e.code === Rembus.STS_GENERIC_ERROR
    end

    for c in [rb, producer, consumer, unauth_consumer]
        close(c)
    end
    @test bag.msg_received === 1
end

authorized_component = "test_private"

setup() = setup(authorized_component)

execute(() -> run(authorized_component), "test_private_topic", setup=setup)

rm(Rembus.root_dir(), recursive=true)
