module DuckDBExt

using DataFrames
using Dates
using DuckDB
using JSON3
using Rembus

const typemap = Dict(
    "BLOB" => Vector{UInt8},
    "TEXT" => String,
    "UTINYINT" => Int8,
    "SMALLINT" => Int16,
    "INTEGER" => Int32,
    "HUGEINT" => Int128,
    "UTINYINT" => UInt8,
    "USMALLINT" => UInt16,
    "UINTEGER" => UInt32,
    "UBIGINT" => UInt64,
    "UHUGEINT" => UInt128,
    "FLOAT" => Float32,
    "DOUBLE" => Float64,
    "TIMESTAMP" => DateTime
)

const ducklock = ReentrantLock()

function columndef(col::Rembus.Column)
    return "$(col.name) $(col.type)" *
           (col.nullable == false ? " NOT NULL" : "") *
           (col.default !== nothing ? " DEFAULT '$(col.default)'" : "")
end

function columns(tabledef::Rembus.Table)
    return [t.name for t in tabledef.fields]
end

function set_default(tabledef::Rembus.Table, d; add_nullable=true)
    for col in tabledef.fields
        if haskey(d, col.name)
            continue
        elseif col.default !== nothing
            d[col.name] = col.default
        elseif add_nullable && col.nullable == true
            if !(col.name in tabledef.keys)
                d[col.name] = missing
            end
        end
    end
end

function Rembus.boot(router::Rembus.Router, con::DuckDB.DB)
    data_dir = joinpath(Rembus.rembus_dir(), router.id)
    if haskey(ENV, "DATABASE_URL")
        db_name = "postgres:" * ENV["DATABASE_URL"]
    else
        db_name = "$data_dir.ducklake"
    end

    DuckDB.execute(con, "INSTALL ducklake")
    DuckDB.execute(con, "ATTACH 'ducklake:$db_name' AS rl (DATA_PATH '$data_dir')")
    DuckDB.execute(con, "USE rl")

    tables = [
        """
        CREATE TABLE IF NOT EXISTS message (
            name TEXT NOT NULL,
            recv UBIGINT,
            slot UINTEGER,
            qos UTINYINT,
            uid UBIGINT,
            topic TEXT NOT NULL,
            data TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS exposer (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL,
        )""",
        """
        CREATE TABLE IF NOT EXISTS subscriber (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL,
            msg_from DOUBLE
        )""",
        """
        CREATE TABLE IF NOT EXISTS mark (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            mark UBIGINT,
        )""",
        """
        CREATE TABLE IF NOT EXISTS admin (
            name TEXT NOT NULL,
            twin TEXT
        )""",
        """
        CREATE TABLE IF NOT EXISTS topic_auth (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            topic TEXT NOT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS tenant (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            secret TEXT NOT NULL
        )""",
        """
        CREATE TABLE IF NOT EXISTS wait_ack2 (
            name TEXT NOT NULL,
            twin TEXT NOT NULL,
            ts UBIGINT,
            id UBIGINT
        )
        """
    ]

    for table in tables
        DuckDB.execute(con, table)
    end

    for tabledef in values(router.schema)
        tname = tabledef.name
        if isempty(tabledef.fields)
            continue
        end

        fields = join(["$(columndef(t))" for t in tabledef.fields], ",")
        if haskey(tabledef.extras, "recv_ts")
            cn = tabledef.extras["recv_ts"]
            fields *= ", $cn UBIGINT"
        end
        if haskey(tabledef.extras, "slot")
            cn = tabledef.extras["slot"]
            fields *= ", $cn UINTEGER"
        end

        @debug "[DuckDB] CREATE TABLE IF NOT EXISTS $tname ($fields)"
        DuckDB.execute(con, "CREATE TABLE IF NOT EXISTS $tname ($fields)")
    end

    Rembus.load_configuration(router)

    return con
end

function append(con::DuckDB.DB, tabledef::Rembus.Table, df)
    topic = tabledef.name
    format = tabledef.format
    tblfields = columns(tabledef)
    appender = DuckDB.Appender(con, topic)
    for row in eachrow(df)
        values = decode(row.pkt)[end]
        @info "VALUES: $values"
        if format == "key_value"
            obj::Dict{String, Any} = values[1]
            set_default(tabledef, obj, add_nullable=true)
            if all(k -> haskey(obj, k), tblfields)
                fields = [obj[f] for f in tblfields]
            else
                @warn "[$topic] unsaved $obj with mismatched fields"
                continue
            end
        elseif format == "dataframe"
            df = Rembus.dataframe_if_tagvalue(values[1])
            if names(df) == tblfields
                df_extras(tabledef, df, row)
                DuckDB.register_data_frame(con, df, "df_view")
                DuckDB.execute(
                    con,
                    "INSERT INTO $topic SELECT * FROM df_view"
                )
                DuckDB.unregister_data_frame(con, "df_view")
            else
                @warn "[$topic] unsaved $df with mismatched fields"
            end
            continue
        else
            if length(values) == length(tblfields)
                fields = values
            else
                @warn "[$topic] unsaved $values with mismatched fields"
                continue
            end
        end

        add_extras(tabledef, fields, row)
        for field in fields
            DuckDB.append(appender, field)
        end
        DuckDB.end_row(appender)
    end
    DuckDB.close(appender)
end

function add_extras(tabledef::Rembus.Table, fields::Vector, row)
    if haskey(tabledef.extras, "recv_ts")
        push!(fields, row.recv)
    end
    if haskey(tabledef.extras, "slot")
        push!(fields, row.slot)
    end
end

function df_extras(tabledef::Rembus.Table, df, row)
    if haskey(tabledef.extras, "recv_ts")
        df[!, tabledef.extras["recv_ts"]] .= row.recv
    end
    if haskey(tabledef.extras, "slot")
        df[!, tabledef.extras["slot"]] .= row.slot
    end
end

function delete(con::DuckDB.DB, tabledef::Rembus.Table, df)
    tname = tabledef.name
    format = tabledef.format
    for row in eachrow(df)
        data = decode(row.pkt)
        if format == "dataframe"
            df = Rembus.dataframe_if_tagvalue(data[end][1])
            for i in 1:nrow(df)
                conds = String[]
                for key in names(df)
                    push!(conds, "$(key)='$(df[i, key])'")
                end
                cond_str = join(conds, " AND ")
                DuckDB.execute(
                    con,
                    "DELETE FROM $tname WHERE $cond_str"
                )
            end
        else
            obj = data[end][1]
            conds = String[]
            for key in keys(obj)
                push!(conds, "$(key)='$(obj[key])'")
            end
            cond_str = join(conds, " AND ")
            DuckDB.execute(
                con,
                "DELETE FROM $tname WHERE $cond_str"
            )
        end
    end
end

function get_type(col::Rembus.Column)
    if col.nullable
        return Union{typemap[col.type],Missing}
    else
        return typemap[col.type]
    end
end

function upsert(con::DuckDB.DB, tabledef::Rembus.Table, df)
    tname = tabledef.name
    format = tabledef.format
    fields = columns(tabledef)
    indexes = tabledef.keys
    nfields = length(fields)
    types = Any[get_type(t) for t in tabledef.fields]
    if haskey(tabledef.extras, "recv_ts")
        push!(fields, tabledef.extras["recv_ts"])
        push!(types, UInt64)
    end
    if haskey(tabledef.extras, "slot")
        push!(fields, tabledef.extras["slot"])
        push!(types, UInt32)
    end

    tdf = DataFrame(Symbol.(fields) .=> [Vector{T}() for T in types])

    for row in eachrow(df)
        data = decode(row.pkt)
        if format == "key_value"
            obj::Dict{String,Any} = data[end][1]
            set_default(tabledef, obj)
            if haskey(tabledef.extras, "recv_ts")
                obj[tabledef.extras["recv_ts"]] = row.recv
            end
            if haskey(tabledef.extras, "slot")
                obj[tabledef.extras["slot"]] = row.slot
            end

            if all(k -> haskey(obj, k), fields)
                push!(tdf, obj)
            else
                @warn "[$tname] unsaved $obj with one or more missing $fields fields"
                continue
            end
        elseif format == "dataframe"
            df = Rembus.dataframe_if_tagvalue(data[end][1])
            df_extras(tabledef, df, row)
            if names(df) == fields
                append!(tdf, df)
            else
                @warn "[$tname] unsaved $df with mismatched fields"
                continue
            end
        else
            vals = data[end]
            if length(vals) == nfields
                add_extras(tabledef, vals, row)
                push!(tdf, vals)
            else
                @warn "[$tname] unsaved $vals with mismatched fields"
                continue
            end
        end
    end

    isempty(tdf) && return

    tdf = combine(groupby(tdf, indexes)) do g
        g[end, :]  # take the last record of each group
    end

    @debug "[$tname] upserting:\n$tdf"
    DuckDB.register_data_frame(con, tdf, "df_view")
    conds = String[]
    for key in tabledef.keys
        push!(conds, "df_view.$key=$tname.$key")
    end
    cond_str = join(conds, " AND ")

    col_list = join(fields, ", ")
    val_list = join(["df_view.$c" for c in fields], ", ")
    update_list = join(["$c = df_view.$c" for c in setdiff(fields, tabledef.keys)], ", ")
    DuckDB.execute(
        con,
        """MERGE INTO $tname USING df_view ON $cond_str
           WHEN MATCHED THEN UPDATE SET $update_list
           WHEN NOT MATCHED THEN INSERT ($col_list) VALUES ($val_list)
        """
    )
    DuckDB.unregister_data_frame(con, "df_view")
end

function Rembus.save_data_at_rest(router::Rembus.Router, con::DuckDB.DB)
    @debug "[$router] Persisting messages to DuckDB"
    df = select(
        router.msg_df,
        :recv, :slot, :qos, :uid, :topic,
        :pkt => ByRow(p -> JSON3.write(decode(p)[end])) => :data
    )
    df[!, :name] .= router.id
    DuckDB.register_data_frame(con, df, "df_msg")
    DuckDB.execute(
        con,
        "INSERT INTO message SELECT name, recv, slot, qos, uid, topic, data FROM df_msg"
    )
    DuckDB.unregister_data_frame(con, "df_msg")
    for topic in unique(df.topic)
        if haskey(router.schema, topic)
            topicdf = filter(:topic => top -> top == topic, router.msg_df, view=true)
            tabledef = router.schema[topic]
            if tabledef.delete_topic == topic
                delete(con, tabledef, topicdf)
            elseif isempty(tabledef.keys)
                append(con, tabledef, topicdf)
            else
                upsert(con, tabledef, topicdf)
            end
        end
    end

    # clear the in-memory message dataframe
    empty!(router.msg_df)
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
            DuckDB.execute(con, "SELECT * FROM message WHERE name='$name' AND recv>=$min_ts")
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
        DuckDB.unregister_data_frame(con, "df")
    end

    # all rows from new_df that do not have an identical (name, topic, msg_from)
    # combination in current_df
    diff_df = antijoin(new_df, current_df, on=fields)

    if !isempty(diff_df)
        DuckDB.register_data_frame(con, diff_df, "df")
        DuckDB.execute(con, "INSERT INTO $table_name SELECT * from df")
        DuckDB.unregister_data_frame(con, "df")
    end

    #DuckDB.execute(con, "COMMIT")
    #catch e
    #    DuckDB.execute(con, "ROLLBACK")
    #    rethrow(e)
    #end
end

function sync_twin(con, router_name, twin_name, table_name, new_df)
    lock(ducklock)
    current_df = DataFrame(
        DuckDB.execute(
            con,
            "SELECT * FROM $table_name WHERE name='$router_name' AND twin='$twin_name'")
    )
    sync_table(con, table_name, current_df, new_df)
    unlock(ducklock)
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
