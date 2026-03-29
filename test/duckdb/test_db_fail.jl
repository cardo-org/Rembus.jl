using DuckDB
using DataFrames
using Rembus
using Test
using Dates

include("../utils.jl")

#Rembus.debug!()

topic1(name, type, value) = nothing

function run()
    broker_name = "db_fail"
    Rembus.broker_reset(broker_name)

    schema = joinpath(@__DIR__, "test_schema.json")
    bro = component(name=broker_name, schema=schema)
    @info "dburl: $(Rembus.dburl(broker=broker_name))"

    pub = component("duckdb_pub")

    publish(
        pub,
        "topic1",
        "name_a", "type_a", Int8(1), Int16(16), Int32(32), Int64(64),
        slot=1234
    )

    # Simulate a db failure by closing the db connection.
    DBInterface.close!(Rembus.top_router(bro.router).con)

    publish(
        pub,
        "topic1",
        "name_b", "type_b", Int8(1), Int16(16), Int32(32), Int64(64),
        slot=1234
    )

    # wait a litte to let persist task to reopen the connection
    sleep(2)

    df = rpc(pub, "query_topic1")

    close(pub)
    close(bro)

    #con = DuckDB.DB(Rembus.top_router(bro.router).dbpath)
    #df = DuckDB.execute(con, "SELECT * FROM topic1") |> DataFrame
    @debug "[duckdb_schema] topic1 data:\n$(df)"
    @test nrow(df) == 2
    @test df[1, :name] == "name_a"
    @test df[1, :type] == "type_a"
    @test df[1, :tinyint] == 1
    @test df[1, :smallint] == 16
    @test df[1, :integer] == 32
    @test df[1, :bigint] == 64
    @test df[1, :rop] == 1234

end


@info "[db_fail] start"
try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 1
    init_ducklake()
    run()
catch e
    @test false
    @error "[db_fail] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[db_fail] stop"
