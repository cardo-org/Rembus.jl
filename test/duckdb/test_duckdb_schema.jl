using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

topic1(name, type, value) = nothing
topic2(name, type, value) = nothing
topic3(obj) = nothing
topic4(obj) = nothing

function nowts()
    t = Libc.TimeVal().sec
    return UInt32(t - t % 900)
end

function run(con)

    jsonstr = read(joinpath(@__DIR__, "test_schema.json"), String)
    bro = component(con, schema=jsonstr)

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
        "topic2",
        "name", "wrong_number_of_fields"
    )

    publish(
        pub,
        "topic3",
        Dict(
            "name" => "name_a",
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
        Dict("name" => "name_a", "type" => "type_a")
    )

    publish(
        pub,
        "topic4",
        Dict("name" => "name_b", "type" => "type_b")
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
    @test nrow(df) == 2

end


@info "[duckdb_schema] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 1
    init_ducklake()
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
