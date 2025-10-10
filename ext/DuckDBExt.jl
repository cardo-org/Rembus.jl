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

    DuckDB.execute(
        con,
        """
        CREATE TABLE IF NOT EXISTS messages (
            ptr UBIGINT,
            qos UTINYINT,
            uid UHUGEINT,
            topic TEXT,
            data TEXT
        )
        """
    )

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
    DuckDB.execute(con, "INSERT INTO messages SELECT * FROM df_view")
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
        df = DataFrame(DuckDB.execute(con, "SELECT * FROM messages WHERE ptr>=$min_ts"))
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

function Rembus.save_twin(router, twin, db::DuckDBStore)

end

function Rembus.load_twin(router, twin, db::DuckDBStore)

end

function Rembus.load_tenants(router, db::DuckDBStore)
end

function Rembus.load_admins(router, db::DuckDBStore)
end

function Rembus.save_admins(router, db::DuckDBStore)
end

function Rembus.load_topic_auth(router, db::DuckDBStore)
end

function Rembus.save_topic_auth(router, db::DuckDBStore)
end

function Rembus.load_received_acks(router, component::Rembus.RbURL, db::DuckDBStore)
end

function Rembus.save_received_acks(twin, db::DuckDBStore)
end

end
