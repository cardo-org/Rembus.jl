using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run()
    jsonstr = read(joinpath(@__DIR__, "test_enum_schema.json"), String)
    # Likely to be supported in the future
    # https://ducklake.select/docs/stable/duckdb/unsupported_features
    @test_throws ErrorException component(schema=jsonstr)
end

@info "[duckdb_enum] start"
try
    init_ducklake()
    run()
catch e
    @test false
    @error "[duckdb_enum] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[duckdb_enum] stop"
