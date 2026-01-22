using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run()

    jsonstr = read(joinpath(@__DIR__, "test_schema.json"), String)
    bro = component(name="duckdb_schema", schema=jsonstr)

    pub = component("duckdb_pub")

    rpc(pub, "delete_topic4", Dict("where" => "name='name_b'"))

    close(pub)
    close(bro)

    con = DuckDB.DB(Rembus.top_router(bro.router).dbpath)
    df = DuckDB.execute(con, "SELECT * FROM topic4") |> DataFrame
    close(con)

    @debug "[duckdb_delete] topic4 data:\n$(df)"
    @test nrow(df) == 1

end

@info "[duckdb_delete] start"

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 10
    init_ducklake(reset=false)
    run()
catch e
    @test false
    @error "[duckdb_delete] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[duckdb_delete] stop"
