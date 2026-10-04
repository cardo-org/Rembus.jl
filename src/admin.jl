"""
    isadmin(router, twin, cmd)

Return `true` if `twin` is listed in `router.admins`, i.e. is allowed to
execute the administration command `cmd`.

Logs an `@error` and returns `false` otherwise. Used as a guard at the
beginning of every privileged admin command handler (`private_topic`,
`public_topic`, `authorize`, `unauthorize`, `SHUTDOWN_CMD`,
`BROKER_CONFIG_CMD`, ...).
"""
function isadmin(router, twin, cmd)
    sts = twin.uid.id in router.admins
    if !sts
        @error "$cmd failed: $(rid(twin)) not authorized"
    end

    return sts
end

"""
    isauthorized(router::Router, twin::Twin, topic::AbstractString)

Return `true` if `topic` is public (absent from `router.topic_auth`) or
`twin` is explicitly listed in `router.topic_auth[topic]`.

Every mesh hop re-evaluates this check independently with its own local
`topic_auth` table, so a private topic stays protected regardless of how
many brokers a `subscribe`/`expose`/publish/request message has to cross:
there is no "trusted upstream" shortcut, each router enforces authorization
on its own.
"""
function isauthorized(router::Router, twin::Twin, topic::AbstractString)
    # check if topic is private
    if haskey(router.topic_auth, topic)
        # check if twin is authorized to bind to topic
        if !haskey(router.topic_auth[topic], twin.uid.id)
            return false
        end
    end

    # topic is public or twin is authorized
    return true
end

"""
    private_topic(router, twin, msg)

Administration command handler for `PRIVATE_TOPIC_CMD`: declares
`msg.topic` private by creating an (initially empty) entry in
`router.topic_auth`. Requires `twin` to be an admin (see [`isadmin`](@ref)).

This is a purely local-router operation: it is not flooded across the mesh
by [`mark_and_broadcast`](@ref), so a topic must be declared private on each
broker where it should be protected.
"""
function private_topic(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, PRIVATE_TOPIC_CMD)
        callback_and(Symbol(PRIVATE_TOPIC_HANDLER), router, twin, msg) do
            if !haskey(router.topic_auth, msg.topic)
                router.topic_auth[msg.topic] = Dict()
            end
        end
    else
        sts = STS_GENERIC_ERROR
    end
    return sts
end

"""
    public_topic(router, twin, msg)

Administration command handler for `PUBLIC_TOPIC_CMD`: resets
`msg.topic` back to public by deleting its entry from `router.topic_auth`.
Requires `twin` to be an admin (see [`isadmin`](@ref)).

Like [`private_topic`](@ref), this is a local-router-only operation.
"""
function public_topic(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, PUBLIC_TOPIC_CMD)
        callback_and(Symbol(PUBLIC_TOPIC_HANDLER), router, twin, msg) do
            delete!(router.topic_auth, msg.topic)
        end
    else
        sts = STS_GENERIC_ERROR
    end
    return sts
end

"""
    authorize(router, twin, msg)

Administration command handler for `AUTHORIZE_CMD`: grants the
component identified by `msg.data[CID]` access to `msg.topic`, whether to:

- publish or subscribe to a private topic;
- make RPC requests to, or expose, a remote method.

Requires `twin` to be an admin and `msg.data[CID]` to be a non-empty
component id. The topic is implicitly declared private (an empty
`router.topic_auth[msg.topic]` entry is created if missing) before the
grant is recorded, same as [`private_topic`](@ref) this is local to the
router handling the command.
"""
function authorize(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, AUTHORIZE_CMD) &&
       haskey(msg.data, CID) &&
       !isempty(msg.data[CID])
        callback_and(Symbol(AUTHORIZE_HANDLER), router, twin, msg) do
            if !haskey(router.topic_auth, msg.topic)
                router.topic_auth[msg.topic] = Dict()
            end
            router.topic_auth[msg.topic][msg.data[CID]] = true
        end
    else
        sts = STS_GENERIC_ERROR
    end

    return sts
end

"""
    unauthorize(router, twin, msg)

Administration command handler for `UNAUTHORIZE_CMD`: revokes the
grant previously given with [`authorize`](@ref) for the component
identified by `msg.data[CID]` on `msg.topic`. Requires `twin` to be an
admin and `msg.data[CID]` to be a non-empty component id.
"""
function unauthorize(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, UNAUTHORIZE_CMD) &&
       haskey(msg.data, CID) &&
       !isempty(msg.data[CID])
        callback_and(Symbol(UNAUTHORIZE_HANDLER), router, twin, msg) do
            if haskey(router.topic_auth, msg.topic)
                delete!(router.topic_auth[msg.topic], msg.data[CID])
            end
        end
    else
        sts = STS_GENERIC_ERROR
    end

    return sts
end

"""
    shutdown_broker(router)

Administration action for `SHUTDOWN_CMD`: shut down the broker's
`Visor` supervisor tree, terminating the broker process. Invoked
asynchronously by [`admin_command`](@ref) so the `STS_SUCCESS` response can
still be sent back to the caller before the process goes down.
"""
function shutdown_broker(router)
    @debug "shutting down broker ..."
    try
        Visor.shutdown(router.process.supervisor)
    catch e
        @warn "$SHUTDOWN_CMD: $e"
    end
end

"""
    color_admin(tw::Twin, msg)

Send `msg` to the neighbor `tw`, after "coloring" it: append `rid(tw)` to
`msg.data["touch"]` (creating the list if absent) just before transmitting.

This is the mechanism that marks a link as already crossed by an admin
command, so that `tw` (or whichever router/component re-broadcasts the
message further) never bounces it back to a twin whose id is already in
`touch`. See [`admin_broadcast`](@ref) and the
[Mesh Routing and Forwarding](@ref) guide for the full flood/anti-loop
scheme.
"""
function color_admin(tw::Twin, msg)
    @debug "[$tw] coloring admin message $(msg.data)"
    if !haskey(msg.data, "touch")
        msg.data["touch"] = []
    end
    push!(msg.data["touch"], rid(tw))
    transport_send(tw, msg)
end

"""
    admin_broadcast(router::Router, twin::Twin, msg::RembusMsg)

Flood `msg` to every named, open, directly-connected neighbor of `router`
(`router.id_twin`), except:

- `twin` itself, the originator of the command;
- any neighbor whose `rid` is already present in `msg.data["touch"]`
  (already reached through another path — see [`color_admin`](@ref)).

Each twin that is actually sent the message is marked via
[`color_admin`](@ref) before transmission. This is the link-level half of
the mesh flood; the router-level half (stopping re-processing once a
router has already seen the command) is [`mark_and_broadcast`](@ref),
which calls this function.

Finally, if `twin` registered a future for `msg.id` (a direct, synchronous
admin request), it is resolved here with `STS_SUCCESS`, so the originator
gets its response immediately without waiting for the flood to finish
propagating through the rest of the mesh.
"""
function admin_broadcast(router::Router, twin::Twin, msg::RembusMsg)
    @debug "[$(path(twin))] admin command broadcast: $msg"
    touched = haskey(msg.data, "touch") ? msg.data["touch"] : []
    for tw in values(router.id_twin)
        if hasname(tw) && rid(tw) != rid(twin) &&
           !(rid(tw) in touched)
            @debug "[$(path(twin))] broadcasting $msg to $tw"
            if !isopen(tw.socket)
                @debug "[$tw] expose not sent: socket is closed"
            else
                color_admin(tw, msg)
            end
        end
    end

    # resolve the future
    if haskey(twin.socket.direct, msg.id)
        req = twin.socket.direct[msg.id]
        put!(req.future, ResMsg(msg, STS_SUCCESS))
        delete!(twin.socket.direct, msg.id)
    end

    return nothing
end

"""
    mark_and_broadcast(router, twin, msg)

Router-level loop guard around [`admin_broadcast`](@ref) for mesh-wide
`subscribe`/`expose`/`unsubscribe`/`unexpose` propagation.

Stamps `msg.data["rmark"]` with `router.eid`:

- if `router.eid` is already present, this router has already processed
  `msg` (the flood looped back to it through a cycle in the mesh graph);
  the function returns `false` *without* re-broadcasting, so the caller
  must not reapply the corresponding local table update either;
- otherwise `router.eid` is appended, [`admin_broadcast`](@ref) is called
  to flood `msg` to this router's neighbors, and `true` is returned so the
  caller proceeds to update its local `topic_interests`/`topic_impls`.

Together with the per-link `touch` marker used by [`admin_broadcast`](@ref),
this guarantees a command reaches every router in the mesh exactly once,
regardless of topology (tree, ring, or arbitrary graph).
"""
function mark_and_broadcast(router, twin, msg)
    if haskey(msg.data, "rmark")
        if router.eid in msg.data["rmark"]
            # Already traversed, do not add the twin to the map of interests.
            return false
        else
            push!(msg.data["rmark"], router.eid)
        end
    else
        msg.data["rmark"] = [router.eid]
    end
    admin_broadcast(router, twin, msg)
    return true
end

"""
    ismultipath(router)

Return `true` if `router` should maintain mesh routing tables
(`topic_interests`/`topic_impls`) at all, i.e. it could have more than one
outstanding route: it is a real broker or a pool component, as opposed to a
plain single-link component.

Currently always returns `true`; kept as an explicit gate (and extension
point) around every table update in [`admin_command`](@ref) and
[`update_tables`](@ref) so the optimization for non-routing components can
be reintroduced without touching call sites.
"""
function ismultipath(router)
    #return !isempty(router.listeners) || (length(router.id_twin) > 1)
    return true
end

"""
    admin_command(router::Router, twin, msg::AdminReqMsg)

Dispatch and execute an administration command received from `twin`,
based on `msg.data[COMMAND]`. This is the single entry point for every
mesh-routing state change as well as for broker administration (topic
privacy, authorization, configuration, shutdown, logging level).

Mesh-routing-relevant commands:

- `SETUP_CMD`: bulk-register `twin`'s already-known `"subscribers"`
  and `"exposers"` into `router.topic_interests`/`router.topic_impls` (see
  [`update_tables`](@ref) for the symmetric client-side counterpart), then
  reply with `EnableReactiveMsg`. Used to (re)synchronize a link's full
  state in one shot, typically on reconnection
  (see `twin_setup` / [`reconnect`](@ref)).
- `SUBSCRIBE_CMD` / `EXPOSE_CMD` / `UNSUBSCRIBE_CMD` / `UNEXPOSE_CMD`:
  after an [`isauthorized`](@ref) check, propagate the change mesh-wide via
  [`mark_and_broadcast`](@ref) and, only if that router had not already
  processed this command, apply the corresponding local update to
  `router.topic_interests`/`router.topic_impls` (and `twin.msg_from` for
  subscriptions).
- `PRIVATE_TOPICS_CONFIG_CMD`, `PRIVATE_TOPIC_CMD` /
  `PUBLIC_TOPIC_CMD`, `AUTHORIZE_CMD` /
  `UNAUTHORIZE_CMD`: manage `router.topic_auth` (see
  [`private_topic`](@ref), [`public_topic`](@ref), [`authorize`](@ref),
  [`unauthorize`](@ref)).
- `REACTIVE_CMD`, `BROKER_CONFIG_CMD`, `LOAD_CONFIG_CMD`,
  `SAVE_CONFIG_CMD`, `SHUTDOWN_CMD`, `ENABLE_DEBUG_CMD`,
  `DISABLE_DEBUG_CMD`: broker-local administration, gated by
  [`isadmin`](@ref) where privileged.

Returns a `ResMsg` carrying the resulting status and, for query-style
commands, the requested data.
"""
function admin_command(router::Router, twin, msg::AdminReqMsg)
    if !isa(msg.data, Dict) || !haskey(msg.data, COMMAND)
        return ResMsg(twin, msg.id, STS_GENERIC_ERROR, nothing)
    end

    sts = STS_SUCCESS
    data = nothing
    cmd = msg.data[COMMAND]
    if cmd == SETUP_CMD
        # register the route only if it is a broker
        if ismultipath(router)
            for topic in msg.data["subscribers"]
                if haskey(router.topic_interests, topic)
                    push!(router.topic_interests[topic], twin)
                else
                    router.topic_interests[topic] = Set([twin])
                end
                mark_glob_topic!(router, topic)
            end

            for topic in msg.data["exposers"]
                if haskey(router.topic_impls, topic)
                    push!(router.topic_impls[topic], twin)
                else
                    router.topic_impls[topic] = Set([twin])
                end
            end
            return EnableReactiveMsg(msg.id, true)
        end
    elseif cmd == SUBSCRIBE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(SUBSCRIBE_HANDLER), router, twin, msg) do
                if ismultipath(router)
                    if mark_and_broadcast(router, twin, msg)
                        msg_from = get(msg.data, MSG_FROM, Now)
                        twin.msg_from[msg.topic] = msg_from
                        if haskey(router.topic_interests, msg.topic)
                            push!(router.topic_interests[msg.topic], twin)
                        else
                            router.topic_interests[msg.topic] = Set([twin])
                        end
                        mark_glob_topic!(router, msg.topic)
                    end
                end
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == EXPOSE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(EXPOSE_HANDLER), router, twin, msg) do
                if ismultipath(router)
                    if mark_and_broadcast(router, twin, msg)
                        if haskey(router.topic_impls, msg.topic)
                            push!(router.topic_impls[msg.topic], twin)
                        else
                            router.topic_impls[msg.topic] = Set([twin])
                        end
                    end
                end
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == UNSUBSCRIBE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(UNSUBSCRIBE_HANDLER), router, twin, msg) do
                if ismultipath(router)
                    if mark_and_broadcast(router, twin, msg)
                        if haskey(router.topic_interests, msg.topic)
                            if twin in router.topic_interests[msg.topic]
                                delete!(router.topic_interests[msg.topic], twin)
                                if isempty(router.topic_interests[msg.topic])
                                    delete!(router.topic_interests, msg.topic)
                                end
                                unmark_glob_topic!(router, msg.topic)
                            else
                                sts = STS_GENERIC_ERROR
                            end
                            # remove from twin configuration
                            if haskey(twin.msg_from, msg.topic)
                                delete!(twin.msg_from, msg.topic)
                            end
                        else
                            sts = STS_GENERIC_ERROR
                        end
                    end
                end
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == UNEXPOSE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(UNEXPOSE_HANDLER), router, twin, msg) do
                if ismultipath(router)
                    if mark_and_broadcast(router, twin, msg)
                        if haskey(router.topic_impls, msg.topic)
                            if twin in router.topic_impls[msg.topic]
                                delete!(router.topic_impls[msg.topic], twin)
                                if isempty(router.topic_impls[msg.topic])
                                    delete!(router.topic_impls, msg.topic)
                                end
                            else
                                sts = STS_GENERIC_ERROR
                            end
                        else
                            sts = STS_GENERIC_ERROR
                        end
                    end
                end
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == PRIVATE_TOPICS_CONFIG_CMD
        if isadmin(router, twin, cmd)
            data = Dict()
            for (topic, cids) in router.topic_auth
                data[topic] = collect(keys(cids))
            end
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == PRIVATE_TOPIC_CMD
        sts = private_topic(router, twin, msg)
    elseif cmd == PUBLIC_TOPIC_CMD
        sts = public_topic(router, twin, msg)
    elseif cmd == AUTHORIZE_CMD
        sts = authorize(router, twin, msg)
    elseif cmd == UNAUTHORIZE_CMD
        sts = unauthorize(router, twin, msg)
    elseif cmd == REACTIVE_CMD
        outcome = callback_and(Symbol(REACTIVE_HANDLER), router, twin, msg) do
            enabled = get(msg.data, STATUS, false)
            if enabled
                return EnableReactiveMsg(msg.id, get(msg.data, MSG_FROM, 0.0))
            else
                twin.reactive = false
                return nothing
            end
        end
        if outcome !== nothing
            return outcome
        end
    elseif cmd === BROKER_CONFIG_CMD
        if isadmin(router, twin, cmd)
            data = router_configuration(router)
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd === LOAD_CONFIG_CMD
        if isadmin(router, twin, cmd)
            load_configuration(router)
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd === SAVE_CONFIG_CMD
        if isadmin(router, twin, cmd)
            save_configuration(router)
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == SHUTDOWN_CMD
        if isadmin(router, twin, cmd)
            @async shutdown_broker(router)
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == ENABLE_DEBUG_CMD
        if isadmin(router, twin, cmd)
            logging("debug")
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == DISABLE_DEBUG_CMD
        if isadmin(router, twin, cmd)
            logging("info")
        else
            sts = STS_GENERIC_ERROR
        end
    else
        @error "invalid admin command: $cmd"
        sts = STS_UNKNOWN_ADMIN_CMD
    end

    return ResMsg(twin, msg.id, sts, data)
end
