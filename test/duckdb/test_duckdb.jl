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

function run()
    con = DuckDB.DB()
    bro = component(con)
    # set admin role for sub component
    push!(bro.router.admins, "duckdb_pub")

    pub = component("duckdb_pub")

    private_topic(pub, "subtopic1")
    private_topic(pub, "subtopic2")
    authorize(pub, "duckdb_pub", "subtopic1")
    authorize(pub, "duckdb_pub", "subtopic2")
    authorize(pub, "duckdb_sub", "subtopic1")
    authorize(pub, "duckdb_sub", "subtopic2")
    authorize(pub, "duckdb_othersub", "subtopic1")

    publish(pub, "subtopic1", 1, slot=nowts())
    publish(pub, "subtopic1", 2, qos=Rembus.QOS1)
    publish(pub, "subtopic1", 3)
    sleep(0.5)

    sub = component("duckdb_sub")
    subscribe(sub, subtopic1, Rembus.LastReceived)
    subscribe(sub, subtopic2, 1)
    expose(sub, service1)
    reactive(sub)

    othersub = component("duckdb_othersub")
    subscribe(othersub, subtopic1, Rembus.LastReceived)
    expose(othersub, service1)
    reactive(othersub)

    # set some received acks awaiting ack2
    tw = bro.router.id_twin["duckdb_pub"]
    tw.ackdf = DataFrame(:ts => UInt64[1], :id => Rembus.Msgid[2])

    close(sub)
    close(othersub)
    close(pub)
    close(bro)

    df = DataFrame(DuckDB.execute(con, "select * from subscriber"))
    @test nrow(df) == 3
    df = DataFrame(DuckDB.execute(con, "select * from exposer"))
    @test nrow(df) == 2
    df = DataFrame(DuckDB.execute(con, "select * from mark"))
    @test nrow(df) == 3
    close(con)
end


@info "[duckdb] start"
try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 1
    if haskey(ENV, "PGDATABASE")
        dbname = "rembus_test"
        user = get(ENV, "PGUSER", "postgres")
        pwd = get(ENV, "PGPASSWORD", "postgres")
        ENV["DATABASE_URL"] = "postgresql://$user:$pwd@127.0.0.1/$dbname"
    end
    run()

    # reload the configuration saved in the previous run
    run()
    sleep(1.5)
catch e
    @test false
    @error "[duckdb] server error: $e"
    showerror(stdout, e, catch_backtrace())
finally
    shutdown()
end
@info "[duckdb] stop"
