"""
    private_topic(router, twin, msg)

Administration command to declare a private topic.
"""
function private_topic(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, PRIVATE_TOPIC_CMD)
        callback_and(Symbol(PRIVATE_TOPIC_CMD), router, twin, msg) do
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

Administration command to reset a topic to public.
"""
function public_topic(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, PUBLIC_TOPIC_CMD)
        callback_and(Symbol(PUBLIC_TOPIC_CMD), router, twin, msg) do
            delete!(router.topic_auth, msg.topic)
        end
    else
        sts = STS_GENERIC_ERROR
    end
    return sts
end

"""
    authorize(router, twin, msg)

Administration command to authorize a component to publish/subscribe to a private topic.
"""
function authorize(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, AUTHORIZE_CMD) &&
       haskey(msg.data, CID) &&
       !isempty(msg.data[CID])
        callback_and(Symbol(AUTHORIZE_CMD), router, twin, msg) do
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

Administration command to unauthorize a component to publish/subscribe to a private topic.
"""
function unauthorize(router, twin, msg)
    sts = STS_SUCCESS
    if isadmin(router, twin, UNAUTHORIZE_CMD) &&
       haskey(msg.data, CID) &&
       !isempty(msg.data[CID])
        callback_and(Symbol(UNAUTHORIZE_CMD), router, twin, msg) do
            if haskey(router.topic_auth, msg.topic)
                delete!(router.topic_auth[msg.topic], msg.data[CID])
            end
        end
    else
        sts = STS_GENERIC_ERROR
    end

    return sts
end

function admin_command(router, twin, msg::AdminReqMsg)
    if !isa(msg.data, Dict) || !haskey(msg.data, COMMAND)
        return AdminResMsg(msg.id, STS_GENERIC_ERROR, nothing)
    end

    sts = STS_SUCCESS
    data = nothing
    cmd = msg.data[COMMAND]
    if cmd == ADD_INTEREST_CMD
        if isauth(router, twin, msg.topic)
            callback_and(Symbol(ADD_INTEREST_CMD), router, twin, msg) do
                retroactive = get(msg.data, RETROACTIVE, true)
                twin.retroactive[msg.topic] = retroactive
                if haskey(router.topic_interests, msg.topic)
                    push!(router.topic_interests[msg.topic], twin)
                else
                    router.topic_interests[msg.topic] = Set([twin])
                end
            end
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == ADD_IMPL_CMD
        if isauth(router, twin, msg.topic)
            callback_and(Symbol(ADD_IMPL_CMD), router, twin, msg) do
                if haskey(router.topic_impls, msg.topic)
                    push!(router.topic_impls[msg.topic], twin)
                else
                    router.topic_impls[msg.topic] = Set([twin])
                end
            end
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == REMOVE_INTEREST_CMD
        if isauth(router, twin, msg.topic)
            callback_and(Symbol(REMOVE_INTEREST_CMD), router, twin, msg) do
                if haskey(router.topic_interests, msg.topic)
                    if twin in router.topic_interests[msg.topic]
                        delete!(router.topic_interests[msg.topic], twin)
                    else
                        sts = STS_GENERIC_ERROR
                    end
                    # remove from twin configuration
                    if haskey(twin.retroactive, msg.topic)
                        delete!(twin.retroactive, msg.topic)
                    end
                else
                    sts = STS_GENERIC_ERROR
                end
            end
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == REMOVE_IMPL_CMD
        if isauth(router, twin, msg.topic)
            callback_and(Symbol(REMOVE_IMPL_CMD), router, twin, msg) do
                if haskey(router.topic_impls, msg.topic)
                    if twin in router.topic_impls[msg.topic]
                        delete!(router.topic_impls[msg.topic], twin)
                    else
                        sts = STS_GENERIC_ERROR
                    end
                else
                    sts = STS_GENERIC_ERROR
                end
            end
        else
            sts = STS_GENERIC_ERROR
        end
    elseif cmd == TOPICS_CONFIG_CMD
        # only admins is authorized
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
        enabled = get(msg.data, STATUS, false)
        if enabled
            start_reactive(twin)
        else
            twin.reactive = false
        end
    elseif cmd === ENABLE_ACK_CMD
        twin.qos = with_ack
    elseif cmd === DISABLE_ACK_CMD
        twin.qos = fast
    elseif cmd === BROKER_CONFIG_CMD
        data = router_configuration(router)
    elseif cmd === LOAD_CONFIG_CMD
        # first save data to disk and then return the configuration
        load_configuration(router)
    elseif cmd === SAVE_CONFIG_CMD
        # first save data to disk and then return the configuration
        save_configuration(router)
    elseif cmd === RESET_ROUTER_CMD
        empty!(router.topic_impls)
        empty!(router.topic_interests)
    elseif cmd == SHUTDOWN_CMD
        @debug "shutting down ..."
        sts = STS_SHUTDOWN
        try
            Visor.shutdown(router.process.supervisor)
        catch e
            @error "$SHUTDOWN_CMD: $e"
        end
    elseif cmd == ENABLE_DEBUG_CMD
        CONFIG.debug_modules = [Rembus, Visor]
    elseif cmd == DISABLE_DEBUG_CMD
        CONFIG.debug_modules = []
    elseif cmd == UPTIME_CMD
        data = uptime(router)
    else
        @error "invalid admin command: $cmd"
        sts = STS_UNKNOWN_ADMIN_CMD
    end

    return ResMsg(msg.id, sts, data)
end