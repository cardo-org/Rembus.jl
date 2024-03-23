using JSON3

include("../utils.jl")

mutable struct TestHolder
    msg_received::Int
    TestHolder() = new(0)
end

function consume(bag, data)
    bag.msg_received += 1
end

function setup(name)
    # add admin privilege to client with name equals to test_private
    fn = joinpath(Rembus.CONFIG.db, "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([name])))
    end
end

function run(authorized_component)
    bag = TestHolder()

    priv_topic = "foo"
    myproducer = "myproducer"
    myconsumer = "myconsumer"
    myunauth = "myunauth"

    rb = tryconnect(authorized_component)

    private_topic(rb, priv_topic)
    authorize(rb, myproducer, priv_topic)
    authorize(rb, myconsumer, priv_topic)

    producer = connect(myproducer)

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
    reactive(consumer)

    publish(producer, priv_topic, "some_data")

    sleep(0.2)
    for c in [rb, producer, consumer, unauth_consumer]
        close(c)
    end
    @test bag.msg_received === 1
end

authorized_component = "test_private"

setup() = setup(authorized_component)

execute(() -> run(authorized_component), "test_private_topic", setup=setup)

#@info "[test_private_topic] start"
#try
#    run()
#    @test msg_received === 1
#    testsummary()
#finally
#    shutdown()
#end
#@info "[test_private_topic] stop"
