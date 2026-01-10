using DuckDB
using DataFrames
using JSON3
using Rembus
using Test
using Dates

include("../utils.jl")

#Rembus.debug!()

function topic_at_rest(df; ctx=ctx, node=node)
    ctx["df"] = df
end

function sub(ctx)
    cli = component("cli")
    inject(cli, ctx)
    subscribe(cli, topic_at_rest, Rembus.LastReceived)
    reactive(cli)
    return cli
end

function run(con)
    ctx = Dict()

    df = DataFrame(
        "name" => ["a", "b", "c"],
        "type" => ["t1", "t2", "t3"],
        "value" => ["val1", "val2", "val3"]
    )

    bro = component(
        con,
        schema=JSON3.write(Dict("tables" => [
            Dict(
                "table" => "topic_at_rest",
                "columns" => [
                    Dict("col" => "name", "type" => "TEXT"),
                    Dict("col" => "type", "type" => "TEXT"),
                    Dict("col" => "value", "type" => "TEXT")
                ],
                "keys" => ["name", "type"]
            )
        ])))

    # for JIT compiler
    cli = sub(ctx)
    close(cli)

    pub = component("duckdb_pub")

    publish(pub, "topic_at_rest", df)

    @debug "[$(now())] closing pub"
    close(pub)

    sleep(2)
    now_dt = now(UTC)
    cli = sub(ctx)

    # test query errors
    @test_throws RpcMethodException rpc(cli, "query_topic_at_rest", Dict("no_where_condition"=>""))
    @test_throws RpcMethodException rpc(cli, "delete_topic_at_rest", Dict("no_where_condition"=>""))

    result = rpc(cli, "query_topic_at_rest", Dict("where"=>"name='a'"))
    @debug "topic_at_rest WHERE:\n$df"

    result = rpc(
        cli,
        "query_topic_at_rest",
        Dict("when"=>Dates.format(now_dt, dateformat"yyyy-mm-dd HH:MM:SS"))
   )
    @debug "topic_at_rest WHEN=now:\n$result"
    @test nrow(result) == 3

    seconds_ago = now_dt - Dates.Second(2)
    result = rpc(
        cli,
        "query_topic_at_rest",
        Dict("when"=>Dates.format(seconds_ago, dateformat"yyyy-mm-dd HH:MM:SS"))
   )
    @debug "topic_at_rest WHEN=3 seconds ago:\n$result"
    @test nrow(result) == 0

    # When is just the current timestamp in epoch seconds.
    result = rpc(
        cli,
        "query_topic_at_rest",
        Dict("when"=>Libc.TimeVal().sec, "where"=>"type='t2'")
    )
    @test nrow(result) == 1

    sleep(1)
    close(cli)
    close(bro)
    df = ctx["df"]
    @debug "topic_at_rest received by subscriber:\n$df"
end

@info "[send_data_at_rest] start"
con = DuckDB.DB()

try
    ENV["REMBUS_ARCHIVER_INTERVAL"] = 0.1
    init_ducklake(reset=true)
    run(con)
catch e
    @error "[send_data_at_rest] server error: $e"
    showerror(stdout, e, catch_backtrace())
    @test false
finally
    shutdown()
    close(con)
end
@info "[send_data_at_rest] stop"
