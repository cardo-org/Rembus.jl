using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run(con)
    jsonstr = read(joinpath(@__DIR__, "test_format.json"), String)
    bro = component(con, schema=jsonstr)

    pub = component("duckdb_pub")

    publish(
        pub,
        "belluno/mysensor/device",
        slot=1234
    )
    sleep(3)

    df = rpc(pub, "query_device")
    @test nrow(df) == 1

    df = rpc(pub, "query_device", Dict("where" => "site='belluno'"))
    @test nrow(df) == 1

    rpc(pub, "delete_device", Dict("where" => "site='not_exist'"))
    df = rpc(pub, "query_device")
    @test nrow(df) == 1

    rpc(pub, "delete_device")
    df = rpc(pub, "query_device")
    @test nrow(df) == 0
end

@info "[duckdb_format] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 0.5
    init_ducklake()
    run(con)
catch e
    @test false
    @error "[duckdb_format] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_format] stop"
