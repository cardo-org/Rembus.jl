using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

#Rembus.debug!()

topic1(name, type, value) = nothing
topic2(name, type, value) = nothing
topic3(obj) = nothing
topic4(obj) = nothing

function nowts()
    t = Libc.TimeVal().sec
    return UInt32(t - t % 900)
end

function run(con)

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
                format="sequence",
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
                format="sequence",
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
                format="key_value",
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
                format="key_value"
            )
        ])

    pub = component("duckdb_pub")

    publish(
        pub,
        "topic1",
        "name_a", "type_a", Int8(1), Int16(16), Int32(32), Int64(64),
        slot=1234
    )
    for i in 1:2
        publish(
            pub,
            "topic2",
            "name_$i", "type_a", UInt8(1), UInt16(16), UInt32(32), UInt64(64)
        )
    end
    publish(
        pub,
        "topic3",
        Dict(
            "name" => "name_a",
            "type" => "type_a",
            "float" => Float32(1.0),
            "double" => Float64(2.0))
    )
    publish(
        pub,
        "topic4",
        Dict("name" => "name_a", "type" => "type_a", "value" => "value_a")
    )
    publish(
        pub,
        "topic4",
        Dict("name" => "name_a", "type" => "type_a", "value" => "value_a")
    )

    # missing values
    publish(pub, "topic1", "name_a", "type_a")
    publish(pub, "topic2", "name_a", "type_a")
    publish(pub, "topic3", Dict("type" => "type_a", "value" => "value_a"))
    publish(pub, "topic4", Dict("value" => "value_a"))

    @debug "[$(now())] closing pub"
    close(pub)
    @debug "[$(now())] closed pub"
    close(bro)

    df = DuckDB.execute(con, "SELECT * FROM topic1") |> DataFrame
    @debug "[duckdb_schema] topic1 data:\n$(df)"
    @test nrow(df) == 1
    @test df[1, :name] == "name_a"
    @test df[1, :type] == "type_a"
    @test df[1, :tinyint] == 1
    @test df[1, :smallint] == 16
    @test df[1, :integer] == 32
    @test df[1, :bigint] == 64
    @test df[1, :rop] == 1234

    df = DuckDB.execute(con, "SELECT * FROM topic2") |> DataFrame
    @debug "[duckdb_schema] topic2 data:\n$(df)"
    @test nrow(df) == 2

    df = DuckDB.execute(con, "SELECT * FROM topic4") |> DataFrame
    @debug "[duckdb_schema] topic4 data:\n$(df)"
    @test nrow(df) == 1

end


@info "[duckdb_schema] start"
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
    @error "[duckdb_schema] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
    close(con)
end
@info "[duckdb_schema] stop"
