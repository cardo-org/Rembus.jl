#=
    load_tenants()

Return the owners dataframe
=#
function load_tenants(router)
    fn = joinpath(broker_dir(router), TENANTS_FILE)
    db = DuckDB.DB()
    try
        if isfile(fn)
            @debug "loading file $fn"
            df = DataFrame(DuckDB.query(db, "SELECT * FROM read_json('$fn')"))
            if !isempty(df)
                # if not found then set default tenant
                if columnindex(df, :tenant) == 0
                    @debug "setting default tenant: [$(router.process.supervisor.id)]"
                    df[!, :tenant] .= router.process.supervisor.id
                end
                return df
            end
        end
        @debug "$TENANTS_FILE empty/not found"
        return DataFrame(pin=String[], tenant=String[], enabled=Bool[])
    finally
        close(db)
    end
end


#=
    load_servers(router)

Load from disk the servers to connect.
=#
function load_servers(router)
    fn = joinpath(broker_dir(router), "servers.json")
    if isfile(fn)
        content = read(fn, String)
        components = JSON3.read(content, Set{String})
        for cmp in components
            add_node(router, cmp)
        end
    else
        router.servers = Set()
    end

end

function save_servers(router)
    fn = joinpath(broker_dir(router), "servers.json")
    open(fn, "w") do io
        write(io, JSON3.write(router.servers))
    end
end

#=
    save_tenants(router, tenants::AbstractString)

Save the tenants table.
=#
function save_tenants(router, tenants::AbstractString)
    fn = joinpath(broker_dir(router), TENANTS_FILE)
    db = DuckDB.DB()
    try
        open(fn, "w") do f
            write(f, tenants)
        end
    catch e
        rethrow()
    finally
        close(db)
    end
end

#=
    load_token_app(router)

Return the component_owner dataframe
=#
function load_token_app(router)
    fn = joinpath(broker_dir(router), TENANT_COMPONENT)
    db = DuckDB.DB()
    try
        if isfile(fn)
            df = DataFrame(DuckDB.query(db, "SELECT * FROM read_json('$fn')"))
            if !isempty(df)
                return df
            end
        end
        @debug "$TENANT_COMPONENT empty/not found"
        return DataFrame(tenant=String[], component=String[])
    finally
        close(db)
    end
end

#=
    save_token_app(df)

Save the component_owner table.
=#
function save_token_app(router, df)
    fn = joinpath(broker_dir(router), TENANT_COMPONENT)
    db = DuckDB.DB()
    try
        open(fn, "w") do f
            write(f, arraytable(df))
        end
    catch e
        rethrow()
    finally
        close(db)
    end
end

broker_dir(router::Router) = joinpath(CONFIG.rembus_dir, router.process.supervisor.id)
broker_dir(router::Server) = joinpath(CONFIG.rembus_dir, router.process.id)
broker_dir(broker_name::AbstractString) = joinpath(CONFIG.rembus_dir, broker_name)

keystore_dir() = get(ENV, "REMBUS_KEYSTORE", joinpath(CONFIG.rembus_dir, "keystore"))

keys_dir(router::Router) = joinpath(CONFIG.rembus_dir, router.process.supervisor.id, "keys")
keys_dir(router::Server) = joinpath(CONFIG.rembus_dir, router.process.id, "keys")
keys_dir(broker_name::AbstractString) = joinpath(CONFIG.rembus_dir, broker_name, "keys")

messages_dir(r::Router) = joinpath(CONFIG.rembus_dir, r.process.supervisor.id, "messages")
messages_dir(broker::AbstractString) = joinpath(CONFIG.rembus_dir, broker, "messages")

function fullname(basename::AbstractString)
    for format in ["pem", "der"]
        for type in ["rsa", "ecdsa"]
            fn = "$basename.$type.$format"
            if isfile(fn)
                return fn
            end
        end
    end
    return isfile(basename) ? basename : nothing
end

function key_base(router::Router, cid::AbstractString)
    res = joinpath(CONFIG.rembus_dir, router.process.supervisor.id, "keys", cid)
    return res
end

function key_base(server::Server, cid::AbstractString)
    return joinpath(CONFIG.rembus_dir, server.process.id, "keys", cid)
end

function key_base(broker_name::AbstractString, cid::AbstractString)
    return joinpath(CONFIG.rembus_dir, broker_name, "keys", cid)
end

function key_file(router::Router, cid::AbstractString)
    basename = key_base(router, cid)
    return fullname(basename)
end

function key_file(server::Server, cid::AbstractString)
    basename = key_base(server, cid)
    return fullname(basename)
end

function key_file(broker_name::AbstractString, cid::AbstractString)
    basename = key_base(broker_name, cid)
    return fullname(basename)
end

function save_table(router, router_tbl, filename)
    table = Dict()
    for (topic, twins) in router_tbl
        twin_ids = [tw.id for tw in twins if tw.hasname]
        table[topic] = twin_ids
    end
    fn = joinpath(broker_dir(router), filename)
    open(fn, "w") do io
        write(io, JSON3.write(table))
    end
end

function save_impl_table(router)
    @debug "saving exposers table"
    save_table(router, router.topic_impls, "exposers.json")
end

function save_topic_auth_table(router)
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

function save_admins(router)
    @debug "saving admins"
    fn = joinpath(broker_dir(router), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(router.admins))
    end
end

function save_pubkey(router, cid::AbstractString, pubkey, type)
    name = key_base(router, cid)
    format = "der"
    # check if pubkey start with -----BEGIN chars
    if pubkey[1:10] == UInt8[0x2d, 0x2d, 0x2d, 0x2d, 0x2d, 0x42, 0x45, 0x47, 0x49, 0x4e]
        format = "pem"
    end
    if type == SIG_RSA
        typestr = "rsa"
    else
        typestr = "ecdsa"
    end
    fn = "$name.$typestr.$format"
    open(fn, "w") do io
        write(io, pubkey)
    end
end

function remove_pubkey(router, cid::AbstractString)
    fn = key_file(router, cid)
    if fn !== nothing
        rm(fn)
    end
end

function pubkey_file(router, cid::AbstractString)
    fn = key_file(router, cid)

    if fn !== nothing
        return fn
    else
        error("auth failed: unknown $cid")
    end
end

isregistered(router, cid::AbstractString) = key_file(router, cid) !== nothing

function load_impl_table(router)
    @debug "loading exposers table"
    fn = joinpath(broker_dir(router), "exposers.json")
    if isfile(fn)
        content = read(fn, String)
        table = JSON3.read(content, Dict)
        for (topic, twin_ids) in table
            twins = Set{Twin}()
            for tid in twin_ids
                twin = create_twin(tid, router, loopback)
                push!(twins, twin)
            end
            if !isempty(twins)
                router.topic_impls[topic] = twins
            end
        end
    end
end

function load_topic_auth_table(router)
    @debug "loading topic_auth table"
    fn = joinpath(broker_dir(router), "topic_auth.json")
    if isfile(fn)
        content = read(fn, String)
        topics = Dict()
        for (private_topic, cids) in JSON3.read(content, Dict)
            topics[private_topic] = Dict(cids .=> true)
        end
        router.topic_auth = topics
    end
end

function load_admins(router)
    @debug "loading admins"
    fn = joinpath(broker_dir(router), "admins.json")
    if isfile(fn)
        content = read(fn, String)
        router.admins = JSON3.read(content, Set)
    end
end

#=
    load_twins(router)

Instantiates twins that subscribed to one or more topics.
=#
function load_twins(router)
    @debug "loading subscribers table"
    fn = joinpath(broker_dir(router), "subscribers.json")
    if isfile(fn)
        content = read(fn, String)
        twin_topicsdict = JSON3.read(content, Dict, allow_inf=true)
    else
        twin_topicsdict = Dict()
    end

    twins = Dict()
    for (cid, topicsdict) in twin_topicsdict
        twin = create_twin(cid, router, loopback)
        twin.hasname = true
        twin.msg_from = topicsdict

        for topic in keys(topicsdict)
            if haskey(twins, topic)
                push!(twins[topic], twin)
            else
                twins[topic] = Set([twin])
            end
        end
    end

    for (topic, twins) in twins
        router.topic_interests[topic] = twins
    end
end

function save_marks(router)
    @debug "saving twin marks"
    fn = joinpath(broker_dir(router), "twins.json")
    twin_mark = Dict{String,UInt64}("__counter__" => router.mcounter)
    for twin in values(router.id_twin)
        # save only named twins, anonymous twin cannot be msg_from
        if twin.hasname
            twin_mark[twin.id] = twin.mark
        end
    end
    JSON3.write(fn, twin_mark)
end

function load_marks(router)
    @debug "loading twin marks"
    fn = joinpath(broker_dir(router), "twins.json")
    if isfile(fn)
        content = read(fn, String)
        twinid_mark = JSON3.read(content, Dict{String,UInt64})
        router.mcounter = pop!(twinid_mark, "__counter__")
        for (id, mark) in twinid_mark
            if haskey(router.id_twin, id)
                router.id_twin[id].mark = mark
            end
        end
    end
end

#=
    save_twins(router)

Persist twins to storage.

Save twins configuration only if twin has a name.

Persist undelivered messages if they are queued in memory.
=#
function save_twins(router)
    @debug "saving subscribers table"
    twin_cfg = Dict{String,Dict{String,Float64}}()
    for (twin_id, twin) in router.id_twin
        if twin.hasname
            router.twin_finalize(router.shared, twin)
            # ??? delete!(router.id_twin, twin_id)
            twin_cfg[twin_id] = twin.msg_from
        end
    end
    fn = joinpath(broker_dir(router), "subscribers.json")
    open(fn, "w") do io
        write(io, JSON3.write(twin_cfg, allow_inf=true))
    end
end

#=
    save_configuration(router::Router)

Persist router configuration on disk.
=#
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "saving configuration on disk"
        save_impl_table(router)
        save_topic_auth_table(router)
        save_admins(router)
        save_twins(router)
        save_servers(router)
        save_marks(router)
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "loading configuration from disk"
        load_twins(router)
        load_impl_table(router)
        load_topic_auth_table(router)
        load_admins(router)
        router.owners = load_tenants(router)
        router.component_owner = load_token_app(router)
        load_servers(router)
        load_marks(router)
    end

    router.start_ts = time()
end
