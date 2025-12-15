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
    cli = sub(ctx)

    # test query errors
    @test_throws RpcMethodException rpc(cli, "query_topic_at_rest", Dict("no_where_condition"=>""))
    @test_throws RpcMethodException rpc(cli, "delete_topic_at_rest", Dict("no_where_condition"=>""))

    sleep(1)
    close(cli)
    close(bro)
    df = ctx["df"]
    @info "topic_at_rest:\n$df"
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
