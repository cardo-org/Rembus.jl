include("../utils.jl")

using DataFrames

foo(df) = df

function pool()
    df = DataFrame(:a => 1:3)
    server = connect("server")
    expose(server, foo)

    nodes = ["a", "b"]
    for policy in ["round_robin", "less_busy"]
        rb = component(nodes, policy=policy)
        for round in 1:3
            rpc(rb, "foo", df)
        end
        close(rb)
    end
    close(server)
end

execute(pool, "pool_component")
