using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

function run(con)

    df = DataFrame(
        "name" => ["a", "b", "c"],
        "type" => ["t1", "t2", "t3"],
        "value" => ["val1", "val2", "val3"]
    )

    bro = component(
        con,
        schema=[
            Rembus.Table(
                table="topic1",
                columns=[
                    Rembus.Column("name", "TEXT"),
                    Rembus.Column("type", "TEXT"),
                    Rembus.Column("tinyint", "TINYINT"),
                    Rembus.Column("smallint", "SMALLINT"),
                    Rembus.Column("integer", "INTEGER"),
                    Rembus.Column("bigint", "BIGINT")
                ],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Rembus.Table(
                table="topic2",
                delete_topic="topic2_delete",
                columns=[
                    Rembus.Column("name", "TEXT"),
                    Rembus.Column("type", "TEXT"),
                    Rembus.Column("utinyint", "UTINYINT"),
                    Rembus.Column("usmallint", "USMALLINT"),
                    Rembus.Column("uinteger", "UINTEGER"),
                    Rembus.Column("ubigint", "UBIGINT")],
                keys=["name"],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Rembus.Table(
                table="topic3",
                columns=[
                    Rembus.Column("name", "TEXT"),
                    Rembus.Column("type", "TEXT"),
                    Rembus.Column("float", "FLOAT"),
                    Rembus.Column("double", "DOUBLE")
                ],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )),
            Rembus.Table(
                table="topic4",
                columns=[
                    Rembus.Column("name", "TEXT"),
                    Rembus.Column("type", "TEXT"),
                    Rembus.Column("value", "TEXT")
                ],
                keys=["name", "type"],
                format="dataframe"
            )
        ])

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
