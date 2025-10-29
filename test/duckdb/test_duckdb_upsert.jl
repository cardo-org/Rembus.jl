using DuckDB
using DataFrames
using Rembus
using Test

include("../utils.jl")

subtopic1(val) = nothing
subtopic2() = nothing
service1() = nothing

function nowts()
    t = Libc.TimeVal().sec
    return UInt32(t - t % 900)
end

function run(con)
    ext = Base.get_extension(Rembus, :DuckDBExt)
    tname = "test"
    tabledef = Rembus.Table(
        table=tname,
        columns=[
            Rembus.Column("id", "TEXT", nullable=false),
            Rembus.Column("col1", "INTEGER", default=100),
            Rembus.Column("col2", "TEXT")
        ],
        keys=["id"]
    )
    fields = join(["$(ext.columndef(c))" for c in tabledef.fields], ",")
    DuckDB.execute(con, "CREATE TABLE IF NOT EXISTS $tname ($fields)")

    # Create a dataframe with two records
    msg_df = DataFrame(:pkt => [])
    for i in 1:2
        push!(
            msg_df,
            [
                encode([Rembus.TYPE_PUB, "sensor", ["sen$i", i, "col2_$i"]])
            ]
        )
    end

    # insert the records
    ext.upsert(con, tabledef, msg_df)

    # update sen1 record
    msg_df = DataFrame(
        :pkt => [encode([Rembus.TYPE_PUB, "sensor", ["sen1", 100, "col2_updated"]])]
    )
    ext.upsert(con, tabledef, msg_df)

    # update sen1 record using a key_value format
    tabledef = Rembus.Table(
        table=tname,
        columns=[
            Rembus.Column("id", "TEXT"),
            Rembus.Column("col1", "INTEGER"),
            Rembus.Column("col2", "TEXT")
        ],
        keys=["id"],
        format="key_value"
    )
    msg_df = DataFrame(
        :pkt => [encode([
            Rembus.TYPE_PUB,
            "sensor",
            [Dict("id" => "sen3", "col1" => 1, "col2" => "value")]])]
    )
    ext.upsert(con, tabledef, msg_df)

end


@info "[duckdb_upsert] start"
con = DuckDB.DB()
try
    run(con)
catch e
    @test false
    @error "[duckdb_upsert] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[duckdb_upsert] stop"
