using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run(con)

    jsonstr = read(joinpath(@__DIR__, "test_schema.json"), String)
    bro = component(con, schema=Rembus.schema(jsonstr))

    pub = component("duckdb_pub")

    rpc(pub, "lakedelete", Dict("table" => "topic4", "where" => Dict("name" => "name_b")))

    @test_throws RpcMethodException rpc(pub, "lakedelete", Dict("table" => "topic4"))
    @test_throws RpcMethodException rpc(pub, "lakedelete", Dict("where" => Dict()))
    @test_throws RpcMethodException rpc(
        pub,
        "lakedelete",
        Dict("table" => "invalid_table", "where" => Dict())
    )

    close(pub)
    close(bro)

    df = DuckDB.execute(con, "SELECT * FROM topic4") |> DataFrame
    @debug "[duckdb_delete] topic4 data:\n$(df)"
    @test nrow(df) == 1

end

@info "[duckdb_delete] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 10
    init_ducklake(reset=false)
    run(con)
catch e
    @test false
    @error "[duckdb_delete] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_delete] stop"
