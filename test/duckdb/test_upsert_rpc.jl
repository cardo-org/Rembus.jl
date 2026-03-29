using DuckDB
using DataFrames
using Rembus
using Test

include("../utils.jl")

function run()
    broker_name = "upsert_rpc"
    Rembus.broker_reset(broker_name)
    schema = joinpath(@__DIR__, "test_schema.json")
    bro = component(name=broker_name, schema=schema)


    @info "dburl: $(Rembus.dburl(broker=broker_name))"
    cli = component("duckdb_pub")

    payload = Dict(
        "name" => "name_a",
        "type" => "type_a",
        "tinyint" => Int8(1),
        "smallint" => Int16(16),
        "integer" => Int32(32),
        "bigint" => Int64(64),
    )

    rpc(cli, "upsert_topic1", payload)

    resdf = rpc(cli, "query_topic1")
    @test resdf !== nothing
    @test nrow(resdf) == 1
    @test resdf[1, "name"] == payload["name"]
    @test resdf[1, "type"] == payload["type"]
    @test resdf[1, "tinyint"] == payload["tinyint"]
    @test resdf[1, "smallint"] == payload["smallint"]
    @test resdf[1, "integer"] == payload["integer"]
    @test resdf[1, "bigint"] == payload["bigint"]

    df = DataFrame([k => [v] for (k, v) in payload])
    rpc(cli, "upsert_topic1", df)
    resdf = rpc(cli, "query_topic1")

    @test resdf !== nothing
    @test nrow(resdf) == 2

    payload = Dict(
        "name" => "name_b",
        "type" => "type_b",
        "utinyint" => Int8(1),
        "usmallint" => Int16(16),
        "uinteger" => Int32(32),
        "ubigint" => Int64(64),
    )

    rpc(cli, "upsert_topic2", payload)

    resdf = rpc(cli, "query_topic2")

    @test resdf !== nothing
    @test nrow(resdf) == 1
    @test resdf[1, "name"] == payload["name"]
    @test resdf[1, "type"] == payload["type"]
    @test resdf[1, "utinyint"] == payload["utinyint"]
    @test resdf[1, "usmallint"] == payload["usmallint"]
    @test resdf[1, "uinteger"] == payload["uinteger"]
    @test resdf[1, "ubigint"] == payload["ubigint"]

    df = DataFrame([k => [v] for (k, v) in payload])
    rpc(cli, "upsert_topic2", df)
    resdf = rpc(cli, "query_topic2")

    @test resdf !== nothing
    @test nrow(resdf) == 1

    payload = [
        Dict(
            "name" => "name_1",
        ),
        Dict(
            "name" => "name_2",
        )]
    rpc(cli, "upsert_topic3", payload)
    resdf = rpc(cli, "query_topic3")

    @test resdf !== nothing
    @test nrow(resdf) == 2


    payload = Dict(
        "value" => "astring",
    )
    # missing required name and type fields
    @test_throws RpcMethodException rpc(cli, "upsert_topic4", payload)
    close(cli)
    close(bro)

end

@info "[upsert_rpc] start"
try
    init_ducklake()
    run()
catch e
    @test false
    @error "[upsert_rpc] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[upsert_rpc] stop"
