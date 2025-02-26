using Rembus

const expected_messages = 100_000

mutable struct Session
    count::Int
    done::Condition
    Session() = new(0, Condition())
end

function mytopic(ctx, rb, n)
    ctx.count += 1
    #@info "[mytopic] recv: $n"
    if ctx.count == expected_messages
        notify(ctx.done)
    end
end

function produce(pub)
    #Threads.@spawn for i in 1:expected_messages
    for i in 1:expected_messages
        publish(pub, "mytopic", i)
    end
end

function run()
    ctx = Session()
    pub = connect("pub")
    sub = connect("sub")
    inject(sub, ctx)
    subscribe(sub, mytopic)
    reactive(sub)

    @async produce(pub)
    t = time()
    wait(ctx.done)
    t = time() - t
    @info "[publish] delta time: $t secs"
end


try
    bro = broker(ws=8000)
    run()
catch e
    @error "[publish]: $e"
finally
    shutdown()
end
