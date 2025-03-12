#=
    isadmin(router, twin, cmd)
Check if twin client has admin privilege.
=#
function isadmin(router, twin, cmd)
    sts = twin.uid.id in router.admins
    if !sts
        @error "$cmd failed: $(tid(twin)) not authorized"
    end

    return sts
end

#=
    isauthorized(router::Router, twin::Twin, topic::AbstractString)

Return true if topic is public or client is authorized to bind to topic.
=#
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

#=
    private_topic(router, twin, msg)

Administration command to declare a private topic.
=#
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

#=
    public_topic(router, twin, msg)

Administration command to reset a topic to public.
=#
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

#=
    authorize(router, twin, msg)

Administration command to authorize a component:

- to publish
- to subscribe to a private topic.
- to make rpc requests to a remote method
- to expose a method
=#
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

#=
    unauthorize(router, twin, msg)

Administration command to unauthorize a component to publish/subscribe to a private topic.
=#
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

function shutdown_broker(router)
    @debug "shutting down broker ..."
    try
        Visor.shutdown(router.process.supervisor)
        save_configuration(router)
    catch e
        @warn "$SHUTDOWN_CMD: $e"
    end
end

function color_admin(tw::Twin, msg)
    @debug "[$tw] coloring admin message $(msg.data)"
    if !haskey(msg.data, "touch")
        msg.data["touch"] = []
    end
    push!(msg.data["touch"], tid(tw))
    transport_send(tw, msg)
end

function admin_broadcast(router::Router, twin::Twin, msg::RembusMsg)
    @debug "[$(path(twin))] admin command broadcast: $msg"
    touched = haskey(msg.data, "touch") ? msg.data["touch"] : []
    for tw in values(router.id_twin)
        if hasname(tw) && tid(tw) != tid(twin) &&
           !(tid(tw) in touched)
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

function mark_and_broadcast(router, twin, msg)
    if haskey(msg.data, "rmark")
        if router.eid in msg.data["rmark"]
            # Already traversed, do not add the twin to the map of interests.
            return nothing
        else
            push!(msg.data["rmark"], router.eid)
        end
    else
        msg.data["rmark"] = [router.eid]
    end

    admin_broadcast(router, twin, msg)
end

function admin_command(router::Router, twin, msg::AdminReqMsg)
    if !isa(msg.data, Dict) || !haskey(msg.data, COMMAND)
        return ResMsg(twin, msg.id, STS_GENERIC_ERROR, nothing)
    end

    sts = STS_SUCCESS
    data = nothing
    cmd = msg.data[COMMAND]
    if cmd == SUBSCRIBE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(SUBSCRIBE_HANDLER), router, twin, msg) do
                msg_from = get(msg.data, MSG_FROM, Now())
                twin.msg_from[msg.topic] = msg_from
                if haskey(router.topic_interests, msg.topic)
                    push!(router.topic_interests[msg.topic], twin)
                else
                    router.topic_interests[msg.topic] = Set([twin])
                end

                mark_and_broadcast(router, twin, msg)
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == EXPOSE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(EXPOSE_HANDLER), router, twin, msg) do
                if haskey(router.topic_impls, msg.topic)
                    push!(router.topic_impls[msg.topic], twin)
                else
                    router.topic_impls[msg.topic] = Set([twin])
                end

                mark_and_broadcast(router, twin, msg)
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == UNSUBSCRIBE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(UNSUBSCRIBE_HANDLER), router, twin, msg) do
                if haskey(router.topic_interests, msg.topic)
                    if twin in router.topic_interests[msg.topic]
                        delete!(router.topic_interests[msg.topic], twin)
                        if isempty(router.topic_interests[msg.topic])
                            delete!(router.topic_interests, msg.topic)
                        end
                    else
                        sts = STS_GENERIC_ERROR
                    end
                    # remove from twin configuration
                    if haskey(twin.msg_from, msg.topic)
                        delete!(twin.msg_from, msg.topic)
                    end

                    mark_and_broadcast(router, twin, msg)
                else
                    sts = STS_GENERIC_ERROR
                end
            end
        else
            sts = STS_GENERIC_ERROR
            data = "unauthorized"
        end
    elseif cmd == UNEXPOSE_CMD
        if isauthorized(router, twin, msg.topic)
            callback_and(Symbol(UNEXPOSE_HANDLER), router, twin, msg) do
                if haskey(router.topic_impls, msg.topic)
                    if twin in router.topic_impls[msg.topic]
                        delete!(router.topic_impls[msg.topic], twin)
                        if isempty(router.topic_impls[msg.topic])
                            delete!(router.topic_impls, msg.topic)
                        end

                        mark_and_broadcast(router, twin, msg)
                    else
                        sts = STS_GENERIC_ERROR
                    end
                else
                    sts = STS_GENERIC_ERROR
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
                if twin.reactive
                    # forward the message counter to the last message received when online
                    # because these messages get already a chance to be delivered.
                    twin.mark = router.mcounter
                end
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
