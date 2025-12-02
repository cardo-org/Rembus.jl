using DataFrames
using Rembus

const expected_messages = 1_000_000

mutable struct Session
    done::Condition
    Session() = new(Condition())
end

function mytopic(df; ctx, node)
    #@info "[mytopic] recv: $df"
    notify(ctx.done)
end

function produce(pub)
    df = DataFrame(:n => 1:expected_messages)
    publish(pub, "mytopic", df)
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
    @info "[publish_df] delta time: $t secs"
end

try
    bro = broker(ws=8000)
    run()
catch e
    @error "[publish_df]: $e"
finally
    shutdown()
end
