using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run(con)
    jsonstr = read(joinpath(@__DIR__, "test_enum_schema.json"), String)
    # Likely to be supported in the future
    # https://ducklake.select/docs/stable/duckdb/unsupported_features
    @test_throws ErrorException component(con, schema=jsonstr)
end

@info "[duckdb_enum] start"
con = DuckDB.DB()

try
    init_ducklake()
    run(con)
catch e
    @test false
    @error "[duckdb_enum] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_enum] stop"
