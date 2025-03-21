include("../utils.jl")

using DataFrames
using DataStructures
using Printf

Rembus.info!()

struct Result
    cid::String
    value::Any
end

#=
N clients -- broker -- M servers
=#
N = 300
M = N + 1

results = OrderedDict()

function collect_result(ch)
    count = 0
    @info "collecting ..."
    delta = 0
    for msg in ch
        if count == 0
            delta = time()
        end
        count += 1
        results[msg.cid] = msg.value
        if count == N
            delta = time() - delta
            @info "done in $delta secs"
            shutdown()
        end
    end
end

function create_df(dim)
    df = DataFrame()
    for i in 1:dim
        df[!, "x$i"] = 1:dim
    end

    return df
end

function run()
    ch = Channel()

    @async collect_result(ch)

    server = Dict()
    for i in 1:M
        name = @sprintf "tcp://:8001/myserver_%06i" i
        server[i] = component(name)
        ### expose(server[i], "myservice$i", (x) -> x * i)
        expose(server[i], "myservice$i", (x) -> x)
    end

    client = Dict()
    for i in 1:N
        name = @sprintf "tcp://:8001/client_%06i" i
        client[i] = connect(name)
    end

    @info "all connected: starting ..."
    df = create_df(10)
    Threads.@spawn for i in 1:N
        ### result = rpc(client[i], "myservice$i", i)
        result = rpc(client[i], "myservice$i", df)
        try
            put!(ch, Result(tid(client[i]), result))
        catch e
            @error "result: $e"
        end
    end
end

rb = broker(tcp=8001)
run()
wait(rb)
