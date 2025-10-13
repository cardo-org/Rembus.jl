module DuckDBExt

using DataFrames
using DuckDB
using JSON3
using Rembus

# To enable DuckDB persistence, instantiate a Rembus component and call:
# con = Base.get_extension(Rembus, :DuckDBExt).init_db(rb.router)

export init_db

struct DuckDBStore <: Rembus.Archiver
    con::DuckDB.DB
end

function init_db(router::Rembus.Router)
    con = DuckDB.DB()
    DuckDB.execute(con, "INSTALL ducklake")
    DuckDB.execute(con, "ATTACH 'ducklake:rembus.ducklake' AS rl")
    DuckDB.execute(con, "USE rl")

    tables = [
        """
        CREATE TABLE IF NOT EXISTS message (
            ptr UBIGINT,
            qos UTINYINT,
            uid UHUGEINT,
            topic TEXT,
            data TEXT
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS exposer (
            name TEXT,
            twin TEXT,
            topic TEXT,
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS subscriber (
            name TEXT,
            twin TEXT,
            topic TEXT,
            msg_from DOUBLE
            )
        """,
        """
        CREATE TABLE IF NOT EXISTS admin (
            name TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS topic_auth (
            name TEXT,
            topic TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT,
            secret TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS wait_ack2 (
            ts UBIGINT,
            msgid UHUGEINT
        )
        """
    ]

    for table in tables
        DuckDB.execute(con, table)
    end

    Rembus.archiver!(router, DuckDBStore(con))
    return con
end

function Rembus.save_data_at_rest(router::Rembus.Router, db::DuckDBStore)
    @debug "[$router] Persisting messages to DuckDB"
    con = db.con
    df = select(
        router.msg_df,
        :ptr, :qos, :uid, :topic, :pkt => ByRow(p -> JSON3.write(decode(p)[end]))
    )
    DuckDB.register_data_frame(con, df, "df_view")
    DuckDB.execute(con, "INSERT INTO message SELECT * FROM df_view")
end

function encmsg(flags, uid, topic, data)
    if uid == 0
        pkt = encode([Rembus.TYPE_PUB | flags, topic, JSON3.read(data, Any)])
    else
        pkt = encode([
            Rembus.TYPE_PUB | flags, Rembus.id2bytes(uid), topic, JSON3.read(data, Any)
        ])
    end

    return pkt
end


# Send the cached and persisted messages never seen by the component managed by the tween
# send_data_at_rest(twin::Twin, from_msg::Float64)

"""

Send persisted amd cached messages.

`max_period` is a time barrier set by the reactive command.

Valid time period for sending old messages: `[min(twin.topic.msg_from, max_period), now_ts]`
"""
function Rembus.send_data_at_rest(twin::Rembus.Twin, max_period::Float64, db::DuckDBStore)
    if Rembus.hasname(twin) && (max_period > 0.0)
        con = db.con
        min_ts = Rembus.uts() - max_period
        df = DataFrame(DuckDB.execute(con, "SELECT * FROM message WHERE ptr>=$min_ts"))
        @debug "[$twin] messages from disk:\n$df"
        interests = Rembus.twin_topics(twin)
        filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
        @debug "[$twin] filtered:\n$filtered"
        if !isempty(filtered)
            filtered = transform(
                filtered,
                [:qos, :uid, :topic, :data] =>
                    ByRow(((qos, id, topic, data) -> encmsg(qos, id, topic, data))) => :pkt
            )
            Rembus.send_messages(twin, filtered)
        end

        Rembus.from_memory_messages(twin)
    end
end

function sync_table(con, router_name, table_name, new_df)
    current_df = DataFrame(
        DuckDB.execute(con, "SELECT * FROM $table_name WHERE name='$router_name'")
    )
    fields = names(current_df)
    conds = ["df.name=t.name"]
    for field in fields
        push!(conds, "df.$field=t.$field")
    end

    cond_str = join(conds, " AND ")
    @info cond_str

    DuckDB.execute(con, "BEGIN TRANSACTION")
    try

        # all rows from current_df that do not have an identical (name, topic, msg_from)
        # combination in new_df
        diff_df = antijoin(current_df, new_df, on=fields)

        if isempty(diff_df)
            @info "nothing to delete"
        else
            @info "deleting:\n$diff_df"
            DuckDB.register_data_frame(con, diff_df, "df")
            DuckDB.execute(
                con,
                """DELETE FROM $table_name t WHERE EXISTS
                (SELECT 1 FROM df WHERE $cond_str)
                """)
        end

        # all rows from new_df that do not have an identical (name, topic, msg_from)
        # combination in current_df
        diff_df = antijoin(new_df, current_df, on=fields)

        if isempty(diff_df)
            @info "nothing to insert"
        else
            @info "inserting:\n$diff_df"
            DuckDB.register_data_frame(con, diff_df, "df")
            DuckDB.execute(con, "INSERT INTO $table_name SELECT * from df")
        end
        DuckDB.execute(con, "COMMIT")
    catch e
        DuckDB.execute(con, "ROLLBACK")
        rethrow(e)
    end

end


function Rembus.save_twin(router, twin, db::DuckDBStore)
    @info "[$twin] save twin"
    con = db.con

    current_df = DataFrame(
        "name" => router.id,
        "twin" => rid(twin),
        "topic" => collect(keys(twin.msg_from)),
        "msg_from" => collect(Base.values(twin.msg_from))
    )
    @info "current_df:\n$current_df"
    sync_table(con, router.id, "subscriber", current_df)

    # INSERT INTO subscriber VALUES (rid(twin), topic, msg_from)
end

function Rembus.load_twin(router, twin, db::DuckDBStore)
    con = db.con
end

function Rembus.load_tenants(router, db::DuckDBStore)
    con = db.con
end

function Rembus.load_admins(router, db::DuckDBStore)
    con = db.con
end

function Rembus.save_admins(router, db::DuckDBStore)
    con = db.con
end

function Rembus.load_topic_auth(router, db::DuckDBStore)
    con = db.con
end

function Rembus.save_topic_auth(router, db::DuckDBStore)
    con = db.con
end

function Rembus.load_received_acks(router, component::Rembus.RbURL, db::DuckDBStore)
    con = db.con
    return DataFrame(DuckDB.execute(con, "SELECT ts, msgid FROM wait_ack2"))
end

function Rembus.save_received_acks(twin, db::DuckDBStore)
    con = db.con
end

end
