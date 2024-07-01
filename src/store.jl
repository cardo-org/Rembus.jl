#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

function load_pages(twin::Twin)
    tdir = joinpath(twins_dir(twin.router), twin.id)
    pages = []
    if !isdir(tdir)
        mkdir(tdir)
    end
    # Load the latest page if any
    pages = readdir(tdir, join=true, sort=true)
    @debug "[$tdir] files pages: $pages"
    return pages
end

function Pager(twin::Twin)
    if twin.pager !== nothing
        return twin.pager
    end

    pages = load_pages(twin)
    if isempty(pages)
        return Pager()
    else
        # load the latest page in memory
        fn = last(pages)
        ts = parse(Int, basename(fn))
        pager = Pager(IOBuffer(read(fn); read=true, write=true, append=true), ts)
        rm(fn)
        return pager
    end
end

#=
    load_owners()

Return the owners dataframe
=#
function load_owners(router)
    fn = joinpath(broker_dir(router), "owners.csv")
    if isfile(fn)
        DataFrame(CSV.File(fn, types=[String, String, String, Bool]))
    else
        @debug "owners.csv not found, only unauthenticated users allowed"
        DataFrame(pin=String[], uid=String[], name=[], enabled=Bool[])
    end
end

#=
    save_owners(owners_df)

Save the owners table.
=#
function save_owners(router, owners_df)
    fn = joinpath(broker_dir(router), "owners.csv")
    CSV.write(fn, owners_df)
end

#=
    load_token_app(router)

Return the component_owner dataframe
=#
function load_token_app(router)
    fn = joinpath(broker_dir(router), "component_owner.csv")
    if isfile(fn)
        df = DataFrame(CSV.File(fn, types=Dict(1 => String, 2 => String)))
        return df
    else
        @debug "component_owner.csv not found"
        DataFrame(uid=String[], component=String[])
    end
end

#=
    save_token_app(df)

Save the component_owner table.
=#
function save_token_app(router, df)
    fn = joinpath(broker_dir(router), "component_owner.csv")
    CSV.write(fn, df)
end

broker_dir(router::Router) = joinpath(CONFIG.root_dir, router.process.supervisor.id)
broker_dir(router::Embedded) = joinpath(CONFIG.root_dir, router.process.id)
broker_dir(broker_name::AbstractString) = joinpath(CONFIG.root_dir, broker_name)

keystore_dir(router) = get(ENV, "REMBUS_KEYSTORE", joinpath(broker_dir(router), "keystore"))

twins_dir(router::Router) = joinpath(CONFIG.root_dir, router.process.supervisor.id, "twins")
twins_dir(broker_name::AbstractString) = joinpath(CONFIG.root_dir, broker_name, "twins")

keys_dir(router::Router) = joinpath(CONFIG.root_dir, router.process.supervisor.id, "keys")
keys_dir(router::Embedded) = joinpath(CONFIG.root_dir, router.process.id, "keys")
keys_dir(broker_name::AbstractString) = joinpath(CONFIG.root_dir, broker_name, "keys")

function key_file(router::Router, cid::AbstractString)
    return joinpath(CONFIG.root_dir, router.process.supervisor.id, "keys", cid)
end

function key_file(server::Embedded, cid::AbstractString)
    return joinpath(CONFIG.root_dir, server.process.id, "keys", cid)
end

function key_file(broker_name::AbstractString, cid::AbstractString)
    return joinpath(CONFIG.root_dir, broker_name, "keys", cid)
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

function save_pubkey(router, cid::AbstractString, pubkey)
    fn = key_file(router, cid)
    open(fn, "w") do io
        write(io, pubkey)
    end
end

function remove_pubkey(router, cid::AbstractString)
    fn = key_file(router, cid)
    if isfile(fn)
        rm(fn)
    end
end

function pubkey_file(router, cid::AbstractString)
    fn = key_file(router, cid)

    if isfile(fn)
        return fn
    else
        error("auth failed: unknown $cid")
    end
end

isregistered(router, cid::AbstractString) = isfile(key_file(router, cid))

function load_impl_table(router)
    @debug "loading exposers table"
    fn = joinpath(broker_dir(router), "exposers.json")
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
        twin_topicsdict = JSON3.read(content, Dict)
    else
        twin_topicsdict = Dict()
    end

    twins = Dict()
    for (cid, topicsdict) in twin_topicsdict
        twin = create_twin(cid, router)
        twin.hasname = true
        twin.pager = Pager(twin)
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
end

#=
    save_twins(router)

Persist twins to storage.

Save twins configuration only if twin has a name.

Persist undelivered messages if they are queued in memory.
=#
function save_twins(router)
    @debug "saving subscribers table"
    twin_cfg = Dict{String,Dict{String,Bool}}()
    for (twin_id, twin) in router.id_twin
        if twin.hasname
            twin_finalize(router.context, twin)
            delete!(router.id_twin, twin_id)
            twin_cfg[twin_id] = twin.retroactive
        end
    end
    fn = joinpath(broker_dir(router), "subscribers.json")
    open(fn, "w") do io
        write(io, JSON3.write(twin_cfg))
    end
end

function save_page(twin)
    if twin.pager !== nothing && twin.pager.io !== nothing
        seekstart(twin.pager.io)
        open(page_file(twin), create=true, write=true) do f
            write(f, twin.pager.io)
            seekstart(twin.pager.io)
        end
    else
        @debug "[$twin]: no page to save"
    end
end

function park(ctx::Any, twin::Twin, msg::PubSubMsg)
    if !twin.hasname
        # do not persist messages addressed to anonymous components
        return
    end

    pager_io = twin.pager.io
    if pager_io === nothing
        @debug "[$twin] creating new Page"
        twin.pager = Pager(IOBuffer(; write=true, read=true))
        pager_io = twin.pager.io
    end

    io = transport_file_io(msg)
    psize = pager_io.size
    if psize >= REMBUS_PAGE_SIZE
        @debug "[$twin]: saving page on disk"
        save_page(twin)
        twin.pager = Pager(IOBuffer(; write=true, read=true))
        pager_io = twin.pager.io
    end

    write(pager_io, io.data)
end

function getmsg(f)
    mark(f)
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

function unpark_file(twin::Twin, fn::AbstractString)
    @debug "[$twin] unparking $fn"
    content = read(fn)
    io = IOBuffer(content)
    try
        send_cached(twin, io)
        rm(fn)
    catch e
        reset(io)
        @error "[$twin] unparking $fn: $e"
        # write back the remaining messages
        open(fn, write=true) do f
            write(f, io)
        end
        rethrow()
    end
end

function unpark_page(twin::Twin)
    io = twin.pager.io
    io === nothing && return
    @debug "[$twin] unparking page"
    try
        send_cached(twin, io)
        twin.pager.io = nothing
    catch e
        reset(io)
        @error "[$twin] unparking page: $e"
        # write back the remaining messages
        newio = IOBuffer(write=true, read=true)
        write(newio, io)
        twin.pager.io = newio
        rethrow()
    end
end

function send_cached(twin, io)
    count = 0
    seekstart(io)
    while !eof(io)
        msg = getmsg(io)
        count += 1
        retro = get(twin.retroactive, msg.topic, true)
        if retro
            #@mlog("[$(twin.id)] <- $(prettystr(msg))")
            transport_send(twin, twin.socket, msg)
        else
            @debug "[$twin] retroactive=$(retro): skipping msg $msg"
        end
    end
end

function unpark(ctx::Any, twin::Twin)
    if twin.hasname
        @debug "[$twin]: unparking"
        files = load_pages(twin)
        for fn in files
            unpark_file(twin, fn)
        end

        # send the in-memory paged messages
        unpark_page(twin)
        twin.pager.io = nothing
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
    end
end

function load_configuration(router)
    callback_or(router, :load_configuration) do
        @debug "loading configuration from disk"
        load_twins(router)
        load_impl_table(router)
        load_topic_auth_table(router)
        load_admins(router)

        router.owners = load_owners(router)
        router.component_owner = load_token_app(router)
    end
end

#=
function test_file(file::String)
    count = 0
    tdir = twins_dir()
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

function debug_unpark(ctx::Any, twin::Twin)
    if twin.hasname
        @debug "[$twin]: unparking"
        files = load_pages(twin)
        for fn in files
            debug_unpark_file(twin, fn)
        end
    end
end

function debug_cached(twin, io)
    count = 0
    seekstart(io)
    while !eof(io)
        msg = getmsg(io)
        count += 1
        @info "[$twin]: $msg: $(msg.data)"
    end
    @info "tot messages: $count"
end

function debug_unpark_file(twin::Twin, fn::AbstractString)
    @debug "unparking $fn"
    content = read(fn)
    io = IOBuffer(content)
    debug_cached(twin, io)
end
=#
