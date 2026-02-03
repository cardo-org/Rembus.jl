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

dbpath(twin) = top_router(twin.router).dbpath

function closedb(db::DuckDB.DB)
    lock(ducklock)
    try
        return DuckDB.close(db)
    finally
        unlock(ducklock)
    end
end

function with_db(f, router::Router)
    con = router.con

    if !isopen(con)
        return nothing
    end

    lock(ducklock)
    try
        return f(con)
    finally
        unlock(ducklock)
    end
end

function with_db_read(f, router::Router)
    reader = DuckDB.connect(router.con)
    return f(reader)
end

function columndef(col::Column)
    return "$(col.name) $(col.type)" *
           (col.nullable == false ? " NOT NULL" : "") *
           (col.default !== nothing ? " DEFAULT '$(col.default)'" : "")
end

function columns(tabledef::Table)
    return [t.name for t in tabledef.fields]
end

function set_default(row::DataFrameRow, tabledef::Table, d; add_nullable=true)
    for col in tabledef.fields
        name = col.name
        if haskey(d, name)
            continue
        elseif haskey(row, ":" * name)
            d[name] = row[":"*name]
        elseif col.default !== nothing
            d[name] = col.default
        elseif add_nullable && col.nullable == true
            if !(name in tabledef.keys)
                d[name] = missing
            end
        end
    end
end

function delete(router::Router, table, obj)
    with_db(router) do con
        if isnothing(obj)
            DuckDB.execute(con, "DELETE FROM $(table.name)")
        else
            allowed = ("where",)
            bad = filter(k -> !(k in allowed), keys(obj))
            isempty(bad) || error("invalid keys: $(join(bad, ", "))")

            cond = obj["where"]
            DuckDB.execute(con, "DELETE FROM $(table.name) WHERE $cond")
        end
        return "ok"
    end
end

function query(router, table, obj)
    with_db_read(router) do con
        if isnothing(obj)
            df = DataFrame(DuckDB.execute(con, "SELECT * FROM rl.$(table.name)"))
        else
            allowed = ("where", "when")
            bad = filter(k -> !(k in allowed), keys(obj))
            isempty(bad) || error("invalid keys: $(join(bad, ", "))")

            where_cond = ""
            if haskey(obj, "where")
                where_cond = " WHERE " * obj["where"]
            end

            at = ""
            if haskey(obj, "when")
                if Base.isa(obj["when"], Real)
                    dt = unix2datetime(obj["when"])
                    ts = Dates.format(dt, "yyyy-mm-dd HH:MM:SS")
                else
                    ts = obj["when"]
                end
                at = " AT (TIMESTAMP => CAST('$ts' AS TIMESTAMP))"
            end

            @debug "query: SELECT * FROM $(table.name) $at $where_cond"
            df = DataFrame(DuckDB.execute(con, "SELECT * FROM rl.$(table.name) $at $where_cond"))
        end
        return df
    end
end

function dbconnect(; broker="broker", datadir=nothing, dbpath=nothing)
    if isnothing(datadir)
        datadir = joinpath(rembus_dir(), broker)
    end

    if isnothing(dbpath)
        if haskey(ENV, "DUCKLAKE_URL")
            dbpath = ENV["DUCKLAKE_URL"]
        else
            dbpath = "ducklake:$datadir.ducklake"
        end
    end

    lock(ducklock)
    try
        con = DuckDB.DB()
        DuckDB.execute(con, "INSTALL ducklake")
        @debug "[DuckDB] ATTACH '$dbpath' AS rl (DATA_PATH '$datadir')"
        DuckDB.execute(con, "ATTACH '$dbpath' AS rl (DATA_PATH '$datadir')")
        DuckDB.execute(con, "USE rl")
        return con
    finally
        unlock(ducklock)
    end
end


function create_tables(router, con)
    lock(ducklock)
    try
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
                tenant TEXT NOT NULL,
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

        for tabledef in values(router.tables)
            tname = tabledef.name
            fields = ["$(columndef(t))" for t in tabledef.fields]
            if haskey(tabledef.extras, "recv_ts")
                cn = tabledef.extras["recv_ts"]
                push!(fields, "$cn UBIGINT")
            end
            if haskey(tabledef.extras, "slot")
                cn = tabledef.extras["slot"]
                push!(fields, "$cn UINTEGER")
            end

            cols = join(fields, ",")
            @debug "[DuckDB] CREATE TABLE IF NOT EXISTS $tname ($cols)"
            DuckDB.execute(con, "CREATE TABLE IF NOT EXISTS $tname ($cols)")

            router.local_function["delete_$tname"] = (obj = nothing) -> delete(router, tabledef, obj)
            router.local_function["query_$tname"] = (obj = nothing) -> query(router, tabledef, obj)
        end
    finally
        unlock(ducklock)
    end
end

function boot(router::Router)
    db_name = router.dbpath
    data_dir = router.datadir

    con = dbconnect(datadir=data_dir, dbpath=db_name)
    create_tables(router, con)
    router.con = con
end

## See: enum type: likely to be supported in the future:
##    https://ducklake.select/docs/stable/duckdb/unsupported_features
##
#function create_enum(en, con::DuckDB.DB)
#    ename = en["name"]
#    tostrings = ["'$v'" for v in en["values"]]
#    evalues = join(tostrings, ",")
#    DuckDB.execute(
#        con,
#        "CREATE TYPE $ename AS ENUM ($evalues)"
#    )
#    typemap[ename] = Int
#end

# Extract placeholder names from pattern, e.g. ":regione/:loc/temp" → ["regione", "loc"]
function extract_names(pattern::AbstractString)
    return [m.captures[1] for m in eachmatch(r"(:\w+)", pattern)]
end

# Turn pattern into a regex that matches any non-slash substring for each placeholder
function make_regex(pattern::AbstractString)
    return Regex("^" * replace(pattern, r":\w+" => "([^/]+)") * "\$")
end


"""
    expand!(df::DataFrame)

Given a DataFrame `df` containing a topic string column (e.g. `:topic`)
and a pattern column (e.g. `:regexp`) with placeholders like `:regione/:loc/temperature`,
this function extracts the named parts from `topic` according to the `regexp`
and adds them as new columns to `df`.

Example:
    df = DataFrame(
        topic=["veneto/agordo/temperature", "veneto/feltre/temperature"],
        table=["temperature", "temperature"],
        regexp=[":regione/:loc/temperature", ":regione/:loc/temperature"]
    )

    expand!(df, :topic, :regexp)
"""
function expand!(df)
    allcols = Symbol[]
    extracted = [Dict{String,String}() for _ in 1:nrow(df)]

    pattern = df[1, :regexp]
    if !isnothing(pattern)
        for (i, row) in enumerate(eachrow(df))
            topic = row[:topic]
            names = extract_names(pattern)
            re = make_regex(pattern)
            m = match(re, topic)
            if m !== nothing
                for (name, value) in zip(names, m.captures)
                    extracted[i][name] = value
                    push!(allcols, Symbol(name))
                end
            end
        end

        # Add columns (with missing where no match)
        for c in unique(allcols)
            df[!, c] = [get(extracted[i], String(c), missing) for i in 1:nrow(df)]
        end

    end
end

"""
    settable!(router, df)

Given a router with defined tables and a DataFrame `df` containing a `:topic` column,
this function matches each topic against the router's table patterns and assigns
the corresponding table name and pattern to new `:table` and `:regexp` columns in `df`.
If no pattern matches, the topic name itself is used as the table name and `nothing` for the
pattern.
"""
function settable!(router, df)
    schema_tables = [tbl for tbl in values(router.tables)]
    tables = []
    regexps = []
    for msg in eachrow(df)
        match = false
        topic_tokens = split(msg.topic, "/")
        for schema_table in schema_tables
            schema_topic = schema_table.topic
            schema_topic_tokens = split(schema_topic, "/")
            if length(topic_tokens) == length(schema_topic_tokens)
                match = true
                for (idx, schema_token) in enumerate(schema_topic_tokens)
                    if !startswith(schema_token, ":") && schema_token != topic_tokens[idx]
                        match = false
                        break
                    end
                end
                if match
                    push!(tables, schema_table.name)
                    push!(regexps, schema_topic)
                    break
                end
            end
        end

        if !match
            # if nothing match set the table using the topic name
            push!(tables, msg.topic)
            push!(regexps, nothing)
        end
    end
    df[!, :table] = tables
    df[!, :regexp] = regexps
    return nothing
end

function getobj(topic, values)
    if isempty(values)
        return Dict{String,Any}()
    end

    v = values[1]
    return Dict{String,Any}(v)
end

function append(con::DuckDB.DB, tabledef::Table, df)
    topic = tabledef.name
    tblfields = columns(tabledef)
    appender = DuckDB.Appender(con, topic)
    try
        for row in eachrow(df)
            values = row.data
            format = get_format(values, tabledef)
            if format == "key_value"
                obj = getobj(topic, values)
                set_default(row, tabledef, obj, add_nullable=true)
                if all(k -> haskey(obj, k), tblfields)
                    fields = [obj[f] for f in tblfields]
                else
                    @warn "[$topic] unsaved $obj with mismatched fields"
                    continue
                end
            elseif format == "dataframe"
                df = dataframe_if_tagvalue(values[1])
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

            for field in fields
                DuckDB.append(appender, field)
            end
            for field in extras(tabledef, fields, row)
                DuckDB.append(appender, field)
            end
            DuckDB.end_row(appender)
        end
    finally
        DuckDB.close(appender)
    end
end

function extras(tabledef::Table, fields::Vector, row)
    vals = []
    if haskey(tabledef.extras, "recv_ts")
        push!(vals, row.recv)
    end
    if haskey(tabledef.extras, "slot")
        push!(vals, row.slot)
    end

    return vals
end

function df_extras(tabledef::Table, df, row)
    if haskey(tabledef.extras, "recv_ts")
        df[!, tabledef.extras["recv_ts"]] .= row.recv
    end
    if haskey(tabledef.extras, "slot")
        df[!, tabledef.extras["slot"]] .= row.slot
    end
end


function get_type(col::Column)
    if col.nullable
        return Union{typemap[col.type],Missing}
    else
        return typemap[col.type]
    end
end

function get_format(data, table::Table)
    """Return the format of the message data."""
    fmt = "sequence"
    len = length(data)
    if len == 1
        arg = data[1]
        if isa(arg, Dict)
            fmt = "key_value"
        elseif isa(arg, Tag) && arg.id == DATAFRAME_TAG
            fmt = "dataframe"
        end
    elseif len == 0 && contains(table.topic, ":")
        fmt = "key_value"
    end

    return fmt
end

function upsert(con::DuckDB.DB, tabledef::Table, df)
    tname = tabledef.name
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
        data = row.data
        format = get_format(data, tabledef)
        if format == "key_value"
            obj = getobj(tname, data)
            set_default(row, tabledef, obj)
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
            df = dataframe_if_tagvalue(data[1])
            df_extras(tabledef, df, row)
            if names(df) == fields
                append!(tdf, df)
            else
                @warn "[$tname] unsaved $df with mismatched fields"
                continue
            end
        else
            vals = data
            if length(vals) == nfields
                append!(vals, extras(tabledef, vals, row))
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

function save_data_at_rest(router::Router, ::DuckDB.DB)
    if !isopen(router.con)
        @warn "[$router] save_data_at_rest failed: db is closed"
        @info "unsaved messages:\n$(router.msg_df)"
        return nothing
    end

    with_db(router) do con
        @debug "[$router] Persisting messages to DuckDB $con"

        try
            df = select(
                router.msg_df,
                :recv, :slot, :qos, :uid, :topic,
                :pkt => ByRow(p -> tobase64(p)) => :data
            )
            df[!, :name] .= rid(router)
            DuckDB.register_data_frame(con, df, "df_msg")
            DuckDB.execute(
                con,
                "INSERT INTO message SELECT name, recv, slot, qos, uid, topic, data FROM df_msg"
            )
            DuckDB.unregister_data_frame(con, "df_msg")

            df[!, :data] = decode.(router.msg_df.pkt) .|> last
            settable!(router, df)
            for tbl in unique(df.table)
                if haskey(router.tables, tbl)
                    topicdf = filter(:table => t -> t == tbl, df, view=false)
                    expand!(topicdf)
                    tabledef = router.tables[tbl]
                    if isempty(tabledef.keys)
                        append(con, tabledef, topicdf)
                    else
                        upsert(con, tabledef, topicdf)
                    end
                end
            end
        catch e
            @error "[$router] save_data_at_rest: $e"
        finally
            # clear the in-memory message dataframe
            empty!(router.msg_df)
        end
    end
end

function tobase64(data)
    io = IOBuffer()
    serialize(io, data)
    return Base64.base64encode(take!(io))
end

function frombase64(str)
    bytes = Base64.base64decode(str)
    io = IOBuffer(bytes)
    return deserialize(io)
end

"""
    send_data_at_rest(twin::Twin, max_period::Float64, con::DuckDB.DB

Send persisted and cached messages.

`max_period` is a time barrier set by the reactive command.

Valid time period for sending old messages: `[min(twin.topic.msg_from, max_period), now_ts]`
"""
function send_data_at_rest(twin::Twin, max_period::Float64, ::DuckDB.DB)
    with_db(top_router(twin.router)) do con
        if hasname(twin) && (max_period > 0.0)
            name = rid(twin.router)
            min_ts = uts() - max_period
            df = DataFrame(
                DuckDB.execute(
                    con,
                    "SELECT * FROM message WHERE name=? AND recv>=?",
                    [name, min_ts]
                )
            )
            interests = twin_topics(twin)
            filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
            if !isempty(filtered)
                filtered = transform(
                    filtered,
                    :data => ByRow(data -> frombase64(data)) => :pkt
                )
                send_messages(twin, filtered)
            end

            from_memory_messages(twin)
        end
    end
end

function sync_table(con, table_name, current_df, new_df)
    fields = names(current_df)
    conds = ["df.name=t.name"]
    for field in fields
        push!(conds, "df.$field=t.$field")
    end

    cond_str = join(conds, " AND ")

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

function exposed_topics(router::Router, twin::Twin)
    topics = String[]
    for (topic, twins) in router.topic_impls
        if twin in twins
            push!(topics, topic)
        end
    end

    return topics
end

function save_twin(router, twin, ::DuckDB.DB)
    with_db(router) do con
        tid = rid(twin)
        name = rid(router)
        current_df = DataFrame(
            "name" => name,
            "twin" => tid,
            "topic" => collect(keys(twin.msg_from)),
            "msg_from" => collect(Base.values(twin.msg_from))
        )
        sync_twin(con, name, tid, "subscriber", current_df)

        current_df = DataFrame(
            "name" => name,
            "twin" => tid,
            "topic" => exposed_topics(router, twin),
        )
        sync_twin(con, name, tid, "exposer", current_df)

        current_df = DataFrame(
            "name" => name,
            "twin" => rid(twin),
            "mark" => twin.mark,
        )
        sync_twin(con, name, tid, "mark", current_df)
    end
end

function load_twin(router, twin, ::DuckDB.DB)
    with_db(router) do con
        name = rid(router)
        df = DataFrame(DuckDB.execute(
            con,
            "SELECT topic, msg_from FROM subscriber WHERE name='$name' AND twin='$(rid(twin))'"
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
            "SELECT topic FROM exposer WHERE name='$name' AND twin='$(rid(twin))'"
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
            "SELECT mark FROM mark WHERE name='$name' AND twin='$(rid(twin))'"
        ))
        if !isempty(df)
            twin.mark = df[1, 1]
        end
    end
end

function load_tenants(router, ::DuckDB.DB)
    with_db(router) do con
        df = DataFrame(DuckDB.execute(
            con,
            "SELECT tenant, secret FROM tenant WHERE name='$(rid(router))'"
        ))
        return Dict(df.tenant .=> df.secret)
    end
end

function load_admins(router, ::DuckDB.DB)
    with_db(router) do con
        df = DataFrame(DuckDB.execute(
            con,
            "SELECT twin FROM admin WHERE name='$(rid(router))'"
        ))
        router.admins = Set{String}(df.twin)
    end
end


function load_topic_auth(router, ::DuckDB.DB)
    with_db(router) do con
        df = DataFrame(
            DuckDB.execute(con, "SELECT twin, topic FROM topic_auth WHERE name='$(rid(router))'")
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
end

function save_topic_auth(router, ::DuckDB.DB)
    with_db(router) do con
        name = rid(router)
        current_df = DataFrame("name" => String[], "twin" => String[], "topic" => String[])

        for (topic, cmp_true) in router.topic_auth
            for twin_name in keys(cmp_true)
                push!(current_df, [name, twin_name, topic])
            end
        end
        sync_cfg(con, name, "topic_auth", current_df)
    end
end

#=
    load_received_acks(router::Router, component::RbURL, ::FileStore)

Load from file the ids of received acks of Pub/Sub messages
awaiting Ack2 acknowledgements.
=#
function load_received_acks(router, component::RbURL, ::DuckDB.DB)
    df = with_db(router) do con
        if isopen(con)
            tw = rid(component)
            return DataFrame(
                DuckDB.execute(
                    con,
                    "SELECT ts, id FROM wait_ack2 WHERE name='$(rid(router))' AND twin='$tw'"
                )
            )
        end
    end

    if isnothing(df)
        df = DataFrame("ts" => UInt64[], "id" => UInt64[])
    end

    return df
end


#=
    save_received_acks(rb::RBHandle, ::FileStore)

Save to file the ids of received acks of Pub/Sub messages
waitings Ack2 acknowledgements.
=#
function save_received_acks(twin, ::DuckDB.DB)
    with_db(top_router(twin.router)) do con
        name = rid(twin.router)
        current_df = copy(twin.ackdf)
        insertcols!(current_df, 1, :name .=> name)
        insertcols!(current_df, 2, :twin .=> rid(twin))
        sync_twin(con, name, rid(twin), "wait_ack2", current_df)
    end
end


#=
    save_configuration(router::Router)

Persist router security settings: admins and private topics.
=#
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "[$router] saving configuration to $(broker_dir(router.id))"
        save_topic_auth(router, router.con)
    end
end

#=
    load_configuration(router::Router)

Load from db router security settings: admins and private topics.
=#
function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "[$router] loading configuration from $(broker_dir(router.id))"
        load_topic_auth(router, router.con)
        load_admins(router, router.con)
        router.owners = load_tenants(router, router.con)
    end
end
