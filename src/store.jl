#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

"""
    load_owners()

Return the owners dataframe
"""
function load_owners()
    fn = joinpath(CONFIG.db, "owners.csv")
    if isfile(fn)
        DataFrame(CSV.File(fn, types=[String, String, String, Bool]))
    else
        @debug "owners.csv not found, only unauthenticated users allowed"
        DataFrame(pin=String[], uid=String[], name=[], enabled=Bool[])
    end
end

"""
    save_owners(owners_df)

Save the owners table.
"""
function save_owners(owners_df)
    fn = joinpath(CONFIG.db, "owners.csv")
    CSV.write(fn, owners_df)
end

"""
    load_token_app()

Return the token_app dataframe
"""
function load_token_app()
    fn = joinpath(CONFIG.db, "token_app.csv")
    if isfile(fn)
        df = DataFrame(CSV.File(fn, types=Dict(1 => String, 2 => String)))
        return df
    else
        @debug "token_app.csv not found"
        DataFrame(uid=String[], app=String[])
    end
end

"""
    save_token_app(df)

Save the owners table.
"""
function save_token_app(df)
    fn = joinpath(CONFIG.db, "token_app.csv")
    CSV.write(fn, df)
end

function twindir()
    if !isdir(CONFIG.db)
        mkdir(CONFIG.db)
    end
    twin_dir = joinpath(CONFIG.db, "twins")
    if !isdir(twin_dir)
        mkdir(twin_dir)
    end
    twin_dir
end

function save_table(router_tbl, filename)
    if !isdir(CONFIG.db)
        mkdir(CONFIG.db)
    end
    table = Dict()
    for (topic, twins) in router_tbl
        twin_ids = [tw.id for tw in twins if tw.hasname]
        table[topic] = twin_ids
    end
    fn = joinpath(CONFIG.db, filename)
    open(fn, "w") do io
        write(io, JSON3.write(table))
    end
end

function save_impl_table(router)
    @debug "saving impls table"
    save_table(router.topic_impls, "impls.json")
end

function save_topic_auth_table(router)
    @debug "saving topic_auth table"
    fn = joinpath(CONFIG.db, "topic_auth.json")

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
    fn = joinpath(CONFIG.db, "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(router.admins))
    end
end

function save_pubkey(cid::AbstractString, pubkey)
    fn = joinpath(CONFIG.db, "apps", cid)
    open(fn, "w") do io
        write(io, pubkey)
    end
end

function remove_pubkey(cid::AbstractString)
    fn = joinpath(CONFIG.db, "apps", cid)
    if isfile(fn)
        rm(fn)
    end
end

function pubkey_file(cid::AbstractString)
    fn = joinpath(CONFIG.db, "apps", cid)

    if isfile(fn)
        return fn
    else
        error("auth failed: unknown $cid")
    end
end

isregistered(cid::AbstractString) = isfile(joinpath(CONFIG.db, "apps", cid))

function load_impl_table(router)
    try
        @debug "loading impls table"
        ## load_table(router, "impls_table", router.topic_impls, "impls.json")
        fn = joinpath(CONFIG.db, "impls.json")
        if isfile(fn)
            content = read(fn, String)
            table = JSON3.read(content, Dict)
            for (topic, twin_ids) in table
                twins = Set{Twin}()
                for tid in twin_ids
                    twin = create_twin(tid, router)
                    push!(twins, twin)
                end
                if !isempty(twins)
                    router.topic_impls[topic] = twins
                end
            end
        end
    catch e
        @error "[impls.json] load failed: $e"
    end
end

function load_topic_auth_table(router)
    @debug "loading topic_auth table"
    fn = joinpath(CONFIG.db, "topic_auth.json")
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
    fn = joinpath(CONFIG.db, "admins.json")
    if isfile(fn)
        content = read(fn, String)
        router.admins = JSON3.read(content, Set)
    end
end

"""
    load_twins(router)

Instantiates twins that have one or more interests.
"""
function load_twins(router)
    fn = joinpath(CONFIG.db, "twins.json")
    if isfile(fn)
        content = read(fn, String)
        twin_topicsdict = JSON3.read(content, Dict)
    else
        twin_topicsdict = Dict()
    end

    twins = Dict()
    for (cid, topicsdict) in twin_topicsdict
        twin = create_twin(cid, router)
        twin.hasname = true
        twin.retroactive = topicsdict

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

    twins_dir = joinpath(CONFIG.db, "twins")
    if isdir(twins_dir)
        # each twin has a file with status and setting data
        files = readdir(twins_dir, join=true)
        for twin_id in files
            try
                @debug "loading twin [$twin_id]"
                if isfile(twin_id)
                    tid = basename(twin_id)

                    # delete the unknown twin
                    if !haskey(router.id_twin, tid)
                        @info "deleting [$twin_id]: unknown twin [$tid]"
                        rm(twin_id)
                        continue
                    end
                end
            catch e
                @error "[$twin_id] load failed: $e"
            end
        end
    end

end

"""
    save_twins(router)

Persist twins to storage.

Save twins configuration only if twin has a name.

Persist undelivered messages if they are queued in memory.
"""
function save_twins(router)
    if !isdir(CONFIG.db)
        mkdir(CONFIG.db)
    end
    twin_dir = joinpath(CONFIG.db, "twins")
    if !isdir(twin_dir)
        mkdir(twin_dir)
    end

    twin_cfg = Dict{String,Dict{String,Bool}}()
    for (twin_id, twin) in router.id_twin
        if twin.hasname
            twin_finalize(CONFIG.broker_ctx, twin)
            delete!(router.id_twin, twin_id)
            twin_cfg[twin_id] = twin.retroactive
        end
    end
    fn = joinpath(CONFIG.db, "twins.json")
    open(fn, "w") do io
        write(io, JSON3.write(twin_cfg))
    end
end

function park(ctx::Any, twin::Twin, msg::RembusMsg)
    if !twin.hasname
        # do not persist messages addressed to anonymous components
        return
    end

    try
        if twin.mfile === nothing
            tdir = twindir()
            fn = joinpath(tdir, twin.id)
            twin.mfile = open(fn, "a")
        end
        io = transport_file_io(msg)
        write(twin.mfile, io.data)
        flush(twin.mfile)
    catch e
        @error "[$twin] park_message: $e"
        showerror(stdout, e, catch_backtrace())
        @showerror e
    end
end

function test_file(file::String)
    count = 0
    tdir = twindir()
    fn = joinpath(tdir, file)
    if isfile(fn)
        open(fn, "r") do f
            while !eof(f)
                msg = getmsg(f)
                count += 1
            end
        end
    end
    @info "messages: $count"
end


function getmsg(f)
    lens = read(f, 4)
    len::UInt32 = lens[1] + Int(lens[2]) << 8 + Int(lens[3]) << 16 + Int(lens[4]) << 24
    content = read(f, len)

    io = IOBuffer(maxsize=len)
    write(io, content)
    seekstart(io)
    llmsg = decode(io)
    msg = PubSubMsg(llmsg[1], llmsg[2])
    return msg
end

function upload_to_queue(twin)
    try
        tdir = twindir()
        fn = joinpath(tdir, twin.id)
        if isfile(fn)
            open(fn, "r") do f
                while !eof(f)
                    msg = getmsg(f)
                    @mlog("[$(twin.id)] <- $(prettystr(msg))")
                    enqueue!(twin.mq, msg)
                end
            end
            if twin.mfile !== nothing
                close(twin.mfile)
                twin.mfile = nothing
            end
            # finally delete the file
            @debug "deleting enqueued messages file [$fn]"
            rm(fn)
        end
    catch e
        @error "upload_to_queue: $e"
        @showerror e
    end
end

function unpark(ctx::Any, twin::Twin)
    count = 0
    try
        tdir = twindir()
        fn = joinpath(tdir, twin.id)
        if isfile(fn)
            open(fn, "r") do f
                while !eof(f)
                    msg = getmsg(f)
                    count += 1
                    @mlog("[$(twin.id)] <- $(prettystr(msg))")
                    retro = get(twin.retroactive, msg.topic, true)
                    if retro
                        if (count % 5000) == 0
                            # pause a little to give time at the receiver to
                            # get the ack messages
                            sleep(0.1)
                        end
                        transport_send(twin, twin.sock, msg)
                    else
                        @debug "[$twin] retroactive=$(retro): skipping msg $msg"
                    end
                end
            end
            @debug "[$twin] sent $count cached msgs"
            if twin.mfile !== nothing
                close(twin.mfile)
                twin.mfile = nothing
            end
            # finally delete the file
            @debug "deleting enqueued messages file [$fn]"
            rm(fn)
        end
    catch e
        @error "[$twin] unpark_messages: $e"
        @showerror e
    end
end

"""
    save_configuration(router::Router)

Persist router configuration on disk.
"""
function save_configuration(router::Router)
    callback_or(router, :save_configuration) do
        @debug "saving configuration on disk"
        save_impl_table(router)
        save_topic_auth_table(router)
        save_admins(router)
        save_twins(router)
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "loading configuration from disk"
        load_twins(router)
        load_impl_table(router)
        load_topic_auth_table(router)
        load_admins(router)

        router.owners = load_owners()
        router.token_app = load_token_app()
    end
end