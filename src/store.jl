#=
    load_tenants()

Return the owners dataframe
=#
function load_tenants(router)
    fn = joinpath(broker_dir(router), TENANTS_FILE)
    if isfile(fn)
        json_data = open(fn, "r") do f
            JSON3.read(f)
        end

        df = DataFrame(json_data)
        # if not found then set default tenant
        if columnindex(df, :tenant) == 0
            @debug "setting default tenant: [$(router.process.supervisor.id)]"
            df[!, :tenant] .= router.process.supervisor.id
        end
        return df
    else
        return DataFrame(pin=String[], tenant=String[], enabled=Bool[])
    end
end

#=
    save_tenants(router, tenants::AbstractString)

Save the tenants table.
=#
function save_tenants(router, tenants::AbstractString)
    fn = joinpath(broker_dir(router), TENANTS_FILE)
    open(fn, "w") do f
        write(f, tenants)
    end
end

#=
    load_tenant_component(router)

Return the dataframe that maps tenants with components.
=#
function load_tenant_component(router)
    fn = joinpath(broker_dir(router), TENANT_COMPONENT)
    df = DataFrame(tenant=String[], component=String[])
    if isfile(fn)
        json_data = open(fn, "r") do f
            JSON3.read(f)
        end
        if !isempty(json_data)
            df = DataFrame(json_data)
        end
    end
    return df
end

#=
    save_tenant_component(router, df)

Save the tenant_component table.
=#
function save_tenant_component(router, df)
    fn = joinpath(broker_dir(router), TENANT_COMPONENT)
    open(fn, "w") do f
        write(f, arraytable(df))
    end
end

#=
    load_token_app(router)

Return the component_owner dataframe
=#
function load_token_app(router)
    fn = joinpath(broker_dir(router), TENANT_COMPONENT)
    if isfile(fn)
        json_data = open(fn, "r") do f
            JSON3.read(f)
        end
        if !isempty(json_data)
            return DataFrame(json_data)
        end
    end
    return DataFrame(tenant=String[], component=String[])
end

broker_dir(r::Router) = joinpath(r.settings.rembus_dir, r.process.supervisor.id)
broker_dir(name::AbstractString) = joinpath(rembus_dir(), name)

function keystore_dir()
    return get(ENV, "REMBUS_KEYSTORE", joinpath(rembus_dir(), "keystore"))
end

keys_dir(r::Router) = joinpath(r.settings.rembus_dir, r.process.supervisor.id, "keys")
keys_dir(name::AbstractString) = joinpath(rembus_dir(), name, "keys")

function messages_dir(r::AbstractRouter)
    r = last_downstream(r)
    return joinpath(r.settings.rembus_dir, r.process.supervisor.id, "messages")
end

messages_dir(t::Twin) = messages_dir(last_downstream(t.router))

function messages_dir(broker::AbstractString)
    return joinpath(rembus_dir(), broker, "messages")
end

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
    res = joinpath(router.settings.rembus_dir, router.process.supervisor.id, "keys", cid)
    return res
end

function key_base(broker_name::AbstractString, cid::AbstractString)
    return joinpath(rembus_dir(), broker_name, "keys", cid)
end

function key_file(router::AbstractRouter, cid::AbstractString)
    basename = key_base(router, cid)
    return fullname(basename)
end

function key_file(broker_name::AbstractString, cid::AbstractString)
    basename = key_base(broker_name, cid)
    return fullname(basename)
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
    fn = joinpath(broker_dir(router), "admins.json")
    @debug "loading $fn"
    if isfile(fn)
        content = read(fn, String)
        router.admins = JSON3.read(content, Set)
    end
end

#=
    load_twin(twin)

Load the persisted twin configuration from disk.
=#
function load_twin(twin::Twin)
    @debug "[$twin] loading configuration"
    router = last_downstream(twin.router)
    twinid = rid(twin)
    fn = joinpath(broker_dir(router), "twins", "$twinid.json")
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

#=
Persist router configuration.

at the moment the only persisted value is the pubsub message counter.
=#
function save_router_config(router)
    @debug "saving twin marks"
    fn = joinpath(broker_dir(router), "router.json")
    twin_mark = Dict{String,UInt64}("__counter__" => router.mcounter)
    JSON3.write(fn, twin_mark)
end

function load_router_config(router)
    @debug "loading twin marks"
    fn = joinpath(broker_dir(router), "router.json")
    if isfile(fn)
        content = read(fn, String)
        twinid_mark = JSON3.read(content, Dict{String,UInt64})
        router.mcounter = pop!(twinid_mark, "__counter__")
    end
end

function exposed_topics(router::Router, twin::Twin)
    topics = []
    for (topic, twins) in router.topic_impls
        if twin in twins
            push!(topics, topic)
        end
    end

    return topics
end

function save_twin(router::Router, twin::Twin)
    @debug "[$twin] saving methods configuration"
    twinid = rid(twin)
    twin_cfg = Dict()

    if hasname(twin) && haskey(router.id_twin, twinid) && !isrepl(twin.uid)
        twin_cfg["subscribers"] = twin.msg_from
        twin_cfg["exposers"] = exposed_topics(router, twin)
        twin_cfg["mark"] = twin.mark

        dir = joinpath(broker_dir(router), "twins")
        if !isdir(dir)
            mkpath(dir)
        end

        fn = joinpath(dir, "$twinid.json")
        open(fn, "w") do io
            write(io, JSON3.write(twin_cfg, allow_inf=true))
        end
    end
end

#=
    save_configuration(router::Router)

Persist router configuration on disk.
=#
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "[$router] saving configuration to $(broker_dir(router))"
        save_topic_auth_table(router)
        save_admins(router)

        for twin in values(router.id_twin)
            save_twin(router, twin)
        end
        save_router_config(router)
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "[$router] loading configuration from $(broker_dir(router))"
        load_topic_auth_table(router)
        load_admins(router)
        router.owners = load_tenants(router)
        router.component_owner = load_tenant_component(router)
        load_router_config(router)
    end

    router.start_ts = time()
end
