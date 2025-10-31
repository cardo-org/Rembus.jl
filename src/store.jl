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

#=
    save_tenants(router, tenants::AbstractString)

Save the tenants table.
=#
function save_tenants(router, tenants::Dict)
    fn = joinpath(broker_dir(router), TENANTS_FILE)
    open(fn, "w") do f
        JSON3.write(f, tenants)
    end
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


function exposed_topics(router::Router, twin::Twin)
    topics = String[]
    for (topic, twins) in router.topic_impls
        if twin in twins
            push!(topics, topic)
        end
    end

    return topics
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

#=
    save_configuration(router::Router)

Persist router configuration on disk.
=#
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "[$router] saving configuration to $(broker_dir(router.id))"
        save_topic_auth(router, router.store)
        save_admins(router, router.store)

        # twins configurations are saved in detach(twin)
        #for twin in values(router.id_twin)
        #    save_twin(router, twin, router.store)
        #end
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "[$router] loading configuration from $(broker_dir(router.id))"
        load_topic_auth(router, router.store)
        load_admins(router, router.store)
        router.owners = load_tenants(router, router.store)
    end
end
