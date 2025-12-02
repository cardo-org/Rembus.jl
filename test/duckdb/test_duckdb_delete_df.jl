using DuckDB
using DataFrames
using JSON3
using Rembus
using Test
using Dates

include("../utils.jl")

#Rembus.debug!()

function run(con)

    df = DataFrame(
        "name" => ["a", "b", "c"],
        "type" => ["t1", "t2", "t3"],
        "value" => ["val1", "val2", "val3"]
    )

    bro = component(
        con,
        schema=JSON3.write(Dict("tables" => [
            Dict(
                "table" => "topic1",
                "columns" => [
                    Dict("col" => "name", "type" => "TEXT"),
                    Dict("col" => "type", "type" => "TEXT"),
                    Dict("col" => "tinyint", "type" => "TINYINT"),
                    Dict("col" => "smallint", "type" => "SMALLINT"),
                    Dict("col" => "integer", "type" => "INTEGER"),
                    Dict("col" => "bigint", "type" => "BIGINT")
                ],
                "format" => "dataframe",
                "extras" => Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Dict(
                "table" => "topic2",
                "delete_topic" => "topic2_delete",
                "columns" => [
                    Dict("col" => "name", "type" => "TEXT"),
                    Dict("col" => "type", "type" => "TEXT"),
                    Dict("col" => "utinyint", "type" => "UTINYINT"),
                    Dict("col" => "usmallint", "type" => "USMALLINT"),
                    Dict("col" => "uinteger", "type" => "UINTEGER"),
                    Dict("col" => "ubigint", "type" => "UBIGINT")],
                "keys" => ["name"],
                "format" => "dataframe",
                "extras" => Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Dict(
                "table" => "topic3",
                "columns" => [
                    Dict("col" => "name", "type" => "TEXT"),
                    Dict("col" => "type", "type" => "TEXT"),
                    Dict("col" => "float", "type" => "FLOAT"),
                    Dict("col" => "double", "type" => "DOUBLE")
                ],
                "format" => "dataframe",
                "extras" => Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )),
            Dict(
                "table" => "topic4",
                "columns" => [
                    Dict("col" => "name", "type" => "TEXT"),
                    Dict("col" => "type", "type" => "TEXT"),
                    Dict("col" => "value", "type" => "TEXT")
                ],
                "keys" => ["name", "type"],
                "format" => "dataframe"
            )
        ])))

    pub = component("duckdb_pub")


    df = DataFrame(
        "name" => ["a", "b"],
        "type" => ["t1", "t2"],
    )
    rpc(pub, "lakedelete", Dict("table" => "topic2", "where" => df))

    @debug "[$(now())] closing pub"
    close(pub)
    @debug "[$(now())] closed pub"
    close(bro)

    df = DuckDB.execute(con, "SELECT * FROM topic2") |> DataFrame
    @debug "[duckdb_delete_df] topic2 data:\n$(df)"
    @test nrow(df) == 0

end

@info "[duckdb_delete_df] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 10
    init_ducklake(reset=false)
    run(con)
catch e
    @test false
    @error "[duckdb_delete_df] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_delete_df] stop"
