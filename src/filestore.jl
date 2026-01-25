closedb(::FileStore) = nothing

Base.isopen(::FileStore) = true

lock_msgfile::ReentrantLock = ReentrantLock()

function messages_files(node, from_msg)
    allfiles = msg_files(node)
    nowts = time()
    mdir = messages_dir(node)
    files = filter(allfiles) do fn
        if isa(node, Twin) && parse(Int, fn) <= node.mark
            # The message was already delivered when the
            # component was previously online.
            return false
        else
            ftime = mtime(joinpath(mdir, fn))
            delta = nowts - ftime
            if delta * 1_000_000 > from_msg
                @debug "skip $fn: mtime: $(unix2datetime(ftime)) ($delta secs from now)"
                return false
            end
        end
        return true
    end

    return files
end

"""
    send_data_at_rest(twin::Rembus.Twin, max_period::Float64, ::FileStore

Send persisted and cached messages.

`max_period` is a time barrier set by the reactive command.

Valid time period for sending old messages: `[min(twin.topic.msg_from, max_period), now_ts]`
"""
function send_data_at_rest(twin::Twin, max_period::Float64, ::FileStore)
    if hasname(twin) && (max_period > 0.0)
        files = messages_files(twin, max_period)
        for fn in files
            @debug "loading file [$fn]"
            from_disk_messages(twin, fn)
        end

        # send the cached in-memory messages
        from_memory_messages(twin)
    end

    return nothing
end

#=
    load_received_acks(router::Router, component::RbURL, ::FileStore)

Load from file the ids of received acks of Pub/Sub messages
awaiting Ack2 acknowledgements.
=#
function load_received_acks(router::Router, component::RbURL, ::FileStore)
    if hasname(component)
        path = acks_file(router, component.id)
        if isfile(path)
            return load_object(path)
        end
    end
    return ack_dataframe()
end

#=
    save_received_acks(rb::RBHandle, ::FileStore)

Save to file the ids of received acks of Pub/Sub messages
waitings Ack2 acknowledgements.
=#
function save_received_acks(twin::Twin, ::FileStore)
    router = top_router(twin.router)
    path = acks_file(router, twin.uid.id)
    save_object(path, twin.ackdf)
end

#=
    load_tenants(router, ::FileStore)

Return the owners dataframe
=#
function load_tenants(router, ::FileStore)
    fn = joinpath(broker_dir(router.id), TENANTS_FILE)
    if isfile(fn)
        return JSON3.read(fn, Dict{String,String})
    else
        return Dict{String,String}()
    end
end

function messages_dir(r::AbstractRouter)
    r = top_router(r)
    return joinpath(r.settings.rembus_dir, r.process.supervisor.id, "messages")
end

messages_dir(t::Twin) = messages_dir(top_router(t.router))

function messages_dir(broker::AbstractString)
    return joinpath(rembus_dir(), broker, "messages")
end

messages_fn(router, ts) = joinpath(messages_dir(router), string(ts))

function from_disk_messages(twin::Twin, fn)
    path = joinpath(messages_dir(twin.router), fn)
    lock(lock_msgfile)
    filtered = []
    try
        df = load_object(path)
        interests = twin_topics(twin)
        filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
    finally
        unlock(lock_msgfile)
    end
    if !isempty(filtered)
        filtered.msg = decode.(Vector{UInt8}.(filtered.pkt))
        send_messages(twin, filtered)
    end
end

file_lt(f1, f2) = parse(Int, f1) < parse(Int, f2)

function msg_files(router)
    mdir = messages_dir(router)
    if !isdir(mdir)
        return String[]
    end
    return sort(readdir(mdir), lt=file_lt)
end

msg_files(twin::Twin) = msg_files(twin.router)

function save_data_at_rest(router, ::FileStore)
    fn = messages_fn(router, router.msg_df[end, 1]) # the newest ts value
    @debug "[broker] persisting messages on disk: $fn"
    lock(lock_msgfile)
    try
        save_object(fn, router.msg_df)
    finally
        unlock(lock_msgfile)
    end
end

function load_topic_auth(router, ::FileStore)
    @debug "loading topic_auth table"
    fn = joinpath(broker_dir(router.id), "topic_auth.json")
    if isfile(fn)
        content = read(fn, String)
        topics = Dict()
        for (private_topic, cids) in JSON3.read(content, Dict)
            topics[private_topic] = Dict(cids .=> true)
        end
        router.topic_auth = topics
    end
end

function save_topic_auth(router, ::FileStore)
    @debug "saving topic_auth table"
    fn = joinpath(broker_dir(router), "topic_auth.json")

    d = Dict()
    for (topic, cids) in router.topic_auth
        d[topic] = keys(cids)
    end

    open(fn, "w") do io
        write(io, JSON3.write(d))
    end
end

function load_admins(router, ::FileStore)
    fn = joinpath(broker_dir(router.id), "admins.json")
    @debug "loading $fn"
    if isfile(fn)
        content = read(fn, String)
        router.admins = JSON3.read(content, Set)
    end
end

function save_admins(router, ::FileStore)
    @debug "saving admins"
    fn = joinpath(broker_dir(router), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(router.admins))
    end
end

#=

Return the twin filename, transforming the '/'.
=#
function twin_file(router, name)
    parts = split(name, '/')

    dirs = parts[1:end-1]
    bdir = broker_dir(router)
    twin_dir = joinpath(bdir, "twins", dirs...)

    if !isdir(twin_dir) && !is_uuid4(name)
        mkpath(twin_dir)
    end
    return joinpath(twin_dir, last(parts) * ".json")
end

#=
    load_twin(router::Router, twin::Twin, ::FileStore)

Load the persisted twin configuration from disk.
=#
function load_twin(router::Router, twin::Twin, ::FileStore)
    @debug "[$twin] loading configuration"
    fn = twin_file(router, twin.uid.id)
    if isfile(fn)
        content = read(fn, String)
        cfg = JSON3.read(content, Dict, allow_inf=true)
    else
        cfg = Dict()
    end

    if haskey(cfg, "subscribers")
        topicsdict = cfg["subscribers"]
        twin.msg_from = topicsdict

        topic_interests = router.topic_interests
        for topic in keys(topicsdict)
            if haskey(topic_interests, topic)
                push!(topic_interests[topic], twin)
            else
                topic_interests[topic] = Set([twin])
            end
        end
    end

    if haskey(cfg, "exposers")
        topics = cfg["exposers"]
        topic_impls = router.topic_impls
        for topic in topics
            if haskey(topic_impls, topic)
                push!(topic_impls[topic], twin)
            else
                topic_impls[topic] = Set([twin])
            end
        end
    end

    if haskey(cfg, "mark")
        twin.mark = cfg["mark"]
    end
end


function save_twin(router::Router, twin::Twin, ::FileStore)
    @debug "[$twin] saving methods configuration"
    twinid = rid(twin)
    twin_cfg = Dict()

    if hasname(twin) && haskey(router.id_twin, twinid) && !isrepl(twin.uid)
        twin_cfg["subscribers"] = twin.msg_from
        twin_cfg["exposers"] = exposed_topics(router, twin)
        twin_cfg["mark"] = twin.mark

        if is_uuid4(router.process.supervisor.id)
            @debug "[$twin] is a pool element: skipping twin configuration save"
            return nothing
        end

        fn = twin_file(router, twin.uid.id)
        open(fn, "w") do io
            write(io, JSON3.write(twin_cfg, allow_inf=true))
        end
    end
end
