using DuckDB
using DataFrames
using Rembus
using Test

include("../utils.jl")

subtopic1(val) = nothing
subtopic2() = nothing
service1() = nothing

function nowts()
    t = Libc.TimeVal().sec
    return UInt32(t - t % 900)
end

function create_df()
    df = DataFrame("topic" => [
        "veneto/agordo/temperature",
        "veneto/feltre/temperature",
        "sicilia/taormina/pressure",
        "mytopic",
        "temperature"
    ])

    return df
end

function metric(topic, value; ctx, node)
    @info "[$topic] received: $value"
    ctx[topic] = value
end

function run(con)
    temperature = 20.0
    jsonstr = read(joinpath(@__DIR__, "test_hierarchy.json"), String)
    ctx = Dict()
    bro = component(con, schema=jsonstr)

    sub = component("hsub")
    inject(sub, ctx)
    subscribe(sub, "veneto/*/temperature", metric)
    reactive(sub)

    pub = component("hpub")

    publish(
        pub,
        "veneto/agordo/temperature",
        Dict("value" => temperature, "sensor" => "sensor_123")
    )

    publish(
        pub,
        "veneto/belluno/temperature",
        temperature
    )

    sleep(0.5)
    close(sub)
    close(pub)
    close(bro)

    @test isa(ctx["veneto/agordo/temperature"], Dict)
    @test isa(ctx["veneto/belluno/temperature"], Float64)

    df = DataFrame(DuckDB.execute(con, "select * from temperature"))
    @test nrow(df) == 1
    @test df[1, :regione] == "veneto"
    @test df[1, :loc] == "agordo"
end

@info "[duckdb_hierarchy] start"
con = DuckDB.DB()

try
    init_ducklake()
    run(con)
catch e
    @test false
    @error "[duckdb_hierarchy] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_hierarchy] stop"
