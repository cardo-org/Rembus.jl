module DuckDBExt

using DataFrames
using DuckDB
using JSON3
using Rembus

function Rembus.boot(router::Rembus.Router, con::DuckDB.DB)
    db_name = joinpath(Rembus.rembus_dir(), router.id)
    DuckDB.execute(con, "INSTALL ducklake")
    DuckDB.execute(con, "ATTACH 'ducklake:$db_name.ducklake' AS rl")
    DuckDB.execute(con, "USE rl")

    tables = [
        """
        CREATE TABLE IF NOT EXISTS message (
            name TEXT,
            ptr UBIGINT,
            qos UTINYINT,
            uid UHUGEINT,
            topic TEXT,
            data TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS exposer (
            name TEXT,
            twin TEXT,
            topic TEXT,
        )""",
        """
        CREATE TABLE IF NOT EXISTS subscriber (
            name TEXT,
            twin TEXT,
            topic TEXT,
            msg_from DOUBLE
        )""",
        """
        CREATE TABLE IF NOT EXISTS mark (
            name TEXT,
            twin TEXT,
            mark UBIGINT,
        )""",
        """
        CREATE TABLE IF NOT EXISTS admin (
            name TEXT,
            twin TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS topic_auth (
            name TEXT,
            twin TEXT,
            topic TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT,
            twin TEXT,
            secret TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS wait_ack2 (
            name TEXT,
            twin TEXT,
            ts UBIGINT,
            id UHUGEINT
        )
        """
    ]

    for table in tables
        DuckDB.execute(con, table)
    end

    Rembus.load_configuration(router)

    return con
end

function Rembus.save_data_at_rest(router::Rembus.Router, con::DuckDB.DB)
    @debug "[$router] Persisting messages to DuckDB"
    df = select(
        router.msg_df,
        :ptr, :qos, :uid, :topic, :pkt => ByRow(p -> JSON3.write(decode(p)[end])) => :data
    )
    df[!, :name] .= router.id
    DuckDB.register_data_frame(con, df, "df_view")
    DuckDB.execute(
        con, "INSERT INTO message SELECT name, ptr, qos, uid, topic, data FROM df_view"
    )
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

"""

Send persisted amd cached messages.

`max_period` is a time barrier set by the reactive command.

Valid time period for sending old messages: `[min(twin.topic.msg_from, max_period), now_ts]`
"""
function Rembus.send_data_at_rest(twin::Rembus.Twin, max_period::Float64, con::DuckDB.DB)
    if Rembus.hasname(twin) && (max_period > 0.0)
        name = twin.router.id
        min_ts = Rembus.uts() - max_period
        df = DataFrame(
            DuckDB.execute(con, "SELECT * FROM message WHERE name='$name' AND ptr>=$min_ts")
        )
        interests = Rembus.twin_topics(twin)
        filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
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

function sync_table(con, table_name, current_df, new_df)
    fields = names(current_df)
    conds = ["df.name=t.name"]
    for field in fields
        push!(conds, "df.$field=t.$field")
    end

    cond_str = join(conds, " AND ")

    #DuckDB.execute(con, "BEGIN TRANSACTION")
    #try

    # all rows from current_df that do not have an identical (name, topic, msg_from)
    # combination in new_df
    diff_df = antijoin(current_df, new_df, on=fields)

    if !isempty(diff_df)
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

    if !isempty(diff_df)
        DuckDB.register_data_frame(con, diff_df, "df")
        DuckDB.execute(con, "INSERT INTO $table_name SELECT * from df")
    end
    #DuckDB.execute(con, "COMMIT")
    #catch e
    #    DuckDB.execute(con, "ROLLBACK")
    #    rethrow(e)
    #end
end

function sync_twin(con, router_name, twin_name, table_name, new_df)
    current_df = DataFrame(
        DuckDB.execute(
            con,
            "SELECT * FROM $table_name WHERE name='$router_name' AND twin='$twin_name'")
    )
    sync_table(con, table_name, current_df, new_df)
end

function sync_cfg(con, router_name, table_name, new_df)
    current_df = DataFrame(
        DuckDB.execute(con, "SELECT * FROM $table_name WHERE name='$router_name'")
    )
    sync_table(con, table_name, current_df, new_df)
end


function Rembus.save_twin(router, twin, con::DuckDB.DB)
    tid = rid(twin)
    current_df = DataFrame(
        "name" => router.id,
        "twin" => tid,
        "topic" => collect(keys(twin.msg_from)),
        "msg_from" => collect(Base.values(twin.msg_from))
    )
    sync_twin(con, router.id, tid, "subscriber", current_df)

    current_df = DataFrame(
        "name" => router.id,
        "twin" => tid,
        "topic" => Rembus.exposed_topics(router, twin),
    )
    sync_twin(con, router.id, tid, "exposer", current_df)

    current_df = DataFrame(
        "name" => router.id,
        "twin" => rid(twin),
        "mark" => twin.mark,
    )
    sync_twin(con, router.id, tid, "mark", current_df)
end

function Rembus.load_twin(router, twin, con::DuckDB.DB)
    df = DataFrame(DuckDB.execute(
        con,
        "SELECT topic, msg_from FROM subscriber WHERE name='$(router.id)' AND twin='$(rid(twin))'"
    ))
    twin.msg_from = Dict(df.topic .=> df.msg_from)
    topic_interests = router.topic_interests
    for topic in df.topic
        if haskey(topic_interests, topic)
            push!(topic_interests[topic], twin)
        else
            topic_interests[topic] = Set([twin])
        end
    end

    df = DataFrame(DuckDB.execute(
        con,
        "SELECT topic FROM exposer WHERE name='$(router.id)' AND twin='$(rid(twin))'"
    ))

    topic_impls = router.topic_impls
    for topic in df.topic
        if haskey(topic_impls, topic)
            push!(topic_impls[topic], twin)
        else
            topic_impls[topic] = Set([twin])
        end
    end

    df = DataFrame(DuckDB.execute(
        con,
        "SELECT mark FROM mark WHERE name='$(router.id)' AND twin='$(rid(twin))'"
    ))
    if !isempty(df)
        twin.mark = df[1, 1]
    end
end

function Rembus.load_tenants(router, con::DuckDB.DB)
    df = DataFrame(DuckDB.execute(
        con,
        "SELECT twin, secret FROM tenant WHERE name='$(router.id)'"
    ))
    return Dict(df.twin .=> df.secret)
end

function Rembus.load_admins(router, con::DuckDB.DB)
    df = DataFrame(DuckDB.execute(
        con,
        "SELECT twin FROM admin WHERE name='$(router.id)'"
    ))
    router.admins = Set{String}(df.twin)
end

function Rembus.save_admins(router, con::DuckDB.DB)
    current_df = DataFrame(
        "name" => router.id,
        "twin" => collect(router.admins)
    )
    sync_cfg(con, router.id, "admin", current_df)
end

function Rembus.load_topic_auth(router, con::DuckDB.DB)
    df = DataFrame(
        DuckDB.execute(con, "SELECT twin, topic FROM topic_auth WHERE name='$(router.id)'")
    )
    topics = Dict()
    for (twin_name, topic) in eachrow(df)
        if haskey(topics, topic)
            topics[topic][twin_name] = true
        else
            topics[topic] = Dict(twin_name => true)
        end
    end
    router.topic_auth = topics
end

function Rembus.save_topic_auth(router, con::DuckDB.DB)
    current_df = DataFrame("name" => String[], "twin" => String[], "topic" => String[])

    for (topic, cmp_true) in router.topic_auth
        for twin_name in keys(cmp_true)
            push!(current_df, [router.id, twin_name, topic])
        end
    end
    sync_cfg(con, router.id, "topic_auth", current_df)
end

function Rembus.load_received_acks(router, component::Rembus.RbURL, con::DuckDB.DB)
    tw = rid(component)
    return DataFrame(
        DuckDB.execute(
            con,
            "SELECT ts, id FROM wait_ack2 WHERE name='$(router.id)' AND twin='$tw'"
        )
    )
end

function Rembus.save_received_acks(twin, con::DuckDB.DB)
    router = Rembus.last_downstream(twin.router)
    current_df = copy(twin.ackdf)
    insertcols!(current_df, 1, :name .=> router.id)
    insertcols!(current_df, 2, :twin .=> rid(twin))
    sync_twin(con, router.id, rid(twin), "wait_ack2", current_df)
end

end
