
function eval_file(twin, cbtype, name, path)
    # crete an anonymous module
    mod = Module()
    @debug "[$twin] evaluating $path"
    service_fn = Base.include(mod, path)
    if cbtype == "services"
        expose(twin, name, service_fn)
    else
        subscribe(twin, name, service_fn)
    end
end

function save_callback(router::Router, cbtype, name, content)
    dir = joinpath(broker_dir(router), "src", cbtype)
    path = joinpath(dir, "$name.jl")
    @debug "[$router] saving callback to $path"
    open(path, "w") do f
        write(f, content)
    end
    twin = router.id_twin["__repl__"]
    eval_file(twin, cbtype, name, path)
end

function delete_callback(router::Router, cbtype, name)
    path = joinpath(broker_dir(router), "src", cbtype, "$name.jl")
    twin = router.id_twin["__repl__"]

    if cbtype == "services"
        unexpose(twin, name)
    else
        unsubscribe(twin, name)
    end

    if isfile(path)
        rm(path, force=true)
    end
end

function load_callbacks(twin)
    router = top_router(twin.router)

    for cbtype in ["services", "subscribers"]
        dir = joinpath(broker_dir(top_router(router).id), "src", cbtype)
        @debug "[$router] Loading callbacks from $dir"

        if !isdir(dir)
            mkpath(dir)
        end

        for file in sort(readdir(dir))
            endswith(file, ".jl") || continue

            path = joinpath(dir, file)
            name = splitext(file)[1]

            mod = Module()
            fn = Base.include(mod, path)
            if cbtype == "services"
                expose(twin, name, fn)
            else
                subscribe(twin, name, fn)
            end
        end
    end

    return nothing
end

function broker_dir(router::AbstractRouter)
    r = top_router(router)
    joinpath(r.settings.rembus_dir, r.process.supervisor.id)
end

broker_dir(name::AbstractString) = joinpath(rembus_dir(), name)

function keystore_dir()
    return get(ENV, "REMBUS_KEYSTORE", joinpath(rembus_dir(), "keystore"))
end

keys_dir(r::Router) = joinpath(r.settings.rembus_dir, r.process.supervisor.id, "keys")
keys_dir(name::AbstractString) = joinpath(rembus_dir(), name, "keys")

function messages_dir(r::AbstractRouter)
    r = top_router(r)
    return joinpath(r.settings.rembus_dir, r.process.supervisor.id, "messages")
end

messages_dir(t::Twin) = messages_dir(top_router(t.router))

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



function exposed_topics(router::Router, twin::Twin)
    topics = String[]
    for (topic, twins) in router.topic_impls
        if twin in twins
            push!(topics, topic)
        end
    end

    return topics
end


#=
    save_configuration(router::Router)

Persist router configuration on disk.
=#
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "[$router] saving configuration to $(broker_dir(router.id))"
        save_topic_auth(router)
        save_admins(router)
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "[$router] loading configuration from $(broker_dir(router.id))"
        load_topic_auth(router)
        load_admins(router)
        router.owners = load_tenants(router)
    end
end
