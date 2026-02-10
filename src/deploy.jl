
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

function local_twin(router::Router)
    # Get the repl twin or the solo twin if it is a connector.
    id_twin = router.id_twin

    if haskey(id_twin, "__repl__")
        return id_twin["__repl__"]
    end

    # Return the first (and unique) value in the dictionary
    return id_twin[first(keys(id_twin))]
end

"""
Get the list of distributed services or subscribers.

Arguments:
- router: the router object
- cbtype: callback type ("services" or "subscribers")
- getbody: return the callback code if true
"""
function list_callback(router, cbtype, getbody)
    dir_path = joinpath(broker_dir(router), "src", cbtype)

    files = []

    for filename in readdir(dir_path)
        file_path = joinpath(dir_path, filename)
        if isfile(file_path)
            if getbody
                body = open(file_path) do f
                    read(f, String)
                end
                push!(files, Dict("name" => filename, "content" => body))
            else
                push!(files, Dict("name" => filename))
            end
        end
    end

    return files
end

function add_callback(router::Router, cbtype, cfg)
    if !haskey(cfg, "name")
        error("add $cbtype failed: name is required")
    end
    name = cfg["name"]

    if !haskey(cfg, "content")
        error("add $cbtype failed: $name impl is required")
    end
    content = cfg["content"]
    tag = get(cfg, "tag", "main")

    dir = joinpath(broker_dir(router), "src", cbtype)
    remove_file(dir, name)
    path = joinpath(dir, "$(name)_$tag.jl")
    @debug "[$router] saving callback to $path"
    open(path, "w") do f
        write(f, content)
    end
    twin = local_twin(router)
    eval_file(twin, cbtype, name, path)
end

function remove_file(dir, name)
    for f in readdir(dir)
        if occursin(Regex("^$(name)_.*\\.jl\$"), f)
            full_path = joinpath(dir, f)
            rm(full_path; force=true)
        end
    end
end

function remove_callback(router::Router, cbtype, name)
    dir = joinpath(broker_dir(router), "src", cbtype)
    twin = local_twin(router)

    if cbtype == "services"
        unexpose(twin, name)
    else
        unsubscribe(twin, name)
    end

    remove_file(dir, name)
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
            name = rsplit(splitext(file)[1], "_", limit=2)[1]
            mod = Module()
            fn = Base.include(mod, path)
            @debug "[$router] loading $cbtype callback [$name] from $path"
            if cbtype == "services"
                expose(twin, name, fn)
            else
                subscribe(twin, name, fn)
            end
        end
    end

    return nothing
end
