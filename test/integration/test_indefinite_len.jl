include("../utils.jl")

function collector(ch)
    for count in 1:5
        put!(ch, count)
    end
end

function consume(bag, rb, msg...)
    @info "[consume] msg: $msg"
    for v in msg
        push!(bag.values, v)
    end
end

function consume_undef_length(bag, rb, val...)
    @info "[consume_undef_length] val:$val"
end

mutable struct Bag
    values::Vector
    Bag() = new([])
end

function run()

    bag = Bag()
    iter = Channel(collector)

    pub = connect()

    sub = connect()
    subscribe(sub, consume)
    subscribe(sub, consume_undef_length)
    shared(sub, bag)
    reactive(sub)

    ##publish(pub, "consume", Rembus.UndefLength(iter))
    publish(pub, "consume_undef_length", Rembus.UndefLength("abc"))
    publish(pub, "consume_undef_length", Rembus.UndefLength(["aaa", "bbb"]))
    publish(pub, "consume_undef_length", Rembus.UndefLength(Dict("aaa" => "bbb")))

    sleep(1)
    @info "values: $(bag.values)"
    ##@test length(bag.values) == 5
    close(pub)
    close(sub)
end

execute(run, "test_indefinite_len")
