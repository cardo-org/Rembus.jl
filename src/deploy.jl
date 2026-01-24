
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
