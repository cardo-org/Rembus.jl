using DuckDB
using DataFrames
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
        schema=[
            Rembus.TableDef(
                table="topic1",
                columns=[
                    "name" => "TEXT",
                    "type" => "TEXT",
                    "tinyint" => "TINYINT",
                    "smallint" => "SMALLINT",
                    "integer" => "INTEGER",
                    "bigint" => "BIGINT"
                ],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Rembus.TableDef(
                table="topic2",
                columns=[
                    "name" => "TEXT",
                    "type" => "TEXT",
                    "utinyint" => "UTINYINT",
                    "usmallint" => "USMALLINT",
                    "uinteger" => "UINTEGER",
                    "ubigint" => "UBIGINT"],
                primary_keys=["name"],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )
            ),
            Rembus.TableDef(
                table="topic3",
                columns=[
                    "name" => "TEXT",
                    "type" => "TEXT",
                    "float" => "FLOAT",
                    "double" => "DOUBLE"
                ],
                format="dataframe",
                extras=Dict(
                    "recv_ts" => "ts",
                    "slot" => "rop"
                )),
            Rembus.TableDef(
                table="topic4",
                columns=[
                    "name" => "TEXT",
                    "type" => "TEXT",
                    "value" => "TEXT"
                ],
                primary_keys=["name", "type"],
                format="dataframe"
            )
        ])

    pub = component("duckdb_pub")

    df = DataFrame(
        "name" => ["a", "b"],
        "type" => ["t1", "t2"],
        "tinyint" => [1, 2],
        "smallint" => [16, 32],
        "integer" => [32, 64],
        "bigint" => [64, 128]
    )
    publish(pub, "topic1", df)
    publish(pub, "topic1", df)

    df = DataFrame(
        "name" => ["a", "b"],
        "type" => ["t1", "t2"],
        "utinyint" => [1, 2],
        "usmallint" => [16, 32],
        "uinteger" => [32, 64],
        "ubigint" => [64, 128]
    )
    publish(pub, "topic2", df)


    df = DataFrame(
        "name" => ["a", "b", "c"],
        "type" => ["t1", "t2", "t3"],
        "value" => ["val1", "val2", "val3"]
    )

    publish(pub, "topic4", df)

    df = DataFrame(
        "name" => ["a", "b", "d"],
        "type" => ["t1", "t2", "t4"],
        "value" => ["val100", "val999", "vald"]
    )
    publish(pub, "topic4", df)

    @debug "[$(now())] closing pub"
    close(pub)
    @debug "[$(now())] closed pub"
    close(bro)

    df = DuckDB.execute(con, "SELECT * FROM topic4") |> DataFrame
    @debug "[duckdb_df] topic4 data:\n$(df)"
    @test nrow(df) == 4

    @test filter(r -> r.name == "a" && r.type == "t1", df)[1, :value] == "val100"
    @test filter(r -> r.name == "b" && r.type == "t2", df)[1, :value] == "val999"
    @test filter(r -> r.name == "c" && r.type == "t3", df)[1, :value] == "val3"
    @test filter(r -> r.name == "d" && r.type == "t4", df)[1, :value] == "vald"

end

@info "[duckdb_df] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 10
    if haskey(ENV, "PGDATABASE")
        dbname = "rembus_test"
        user = get(ENV, "PGUSER", "postgres")
        pwd = get(ENV, "PGPASSWORD", "postgres")
        ENV["DATABASE_URL"] = "postgresql://$user:$pwd@127.0.0.1/$dbname"
        Base.run(`dropdb $dbname --if-exists`)
        Base.run(`createdb $dbname`)
    else
        rm(joinpath(Rembus.rembus_dir(), "broker.ducklake"); force=true, recursive=true)
        rm(Rembus.broker_dir("broker"); force=true, recursive=true)
    end
    run(con)
catch e
    @test false
    @error "[duckdb_df] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_df] stop"
