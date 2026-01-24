function alltwins(router)
    r = Rembus.top_router(router)
    return [t for (name, t) in r.id_twin if name !== "__repl__"]
end

function add_plugin(router::AbstractRouter, context)
    upstream!(router, context)
    sv = router.process.supervisor
    startup(sv, context.process)

    for tw in alltwins(router)
        tw.router = context
    end
end

function add_plugin(twin::Twin, context)
    router = twin.router
    add_plugin(router, context)
    twin.router = context
end

setup_twin(r, twin::Twin) = nothing

function bind(router::Router, url=RbURL(protocol=:repl))
    twin = lock(router.lock) do
        df = load_received_acks(router, url, router.con)
        if haskey(router.id_twin, rid(url))
            twin = router.id_twin[rid(url)]
        else
            twin = Twin(url, bottom_router(router))
            twin.ackdf = df
            load_twin(router, twin, router.con)
        end

        if !isdefined(twin, :process) || istaskdone(twin.process.task)
            start_twin(router, twin)
        end

        return twin
    end

    twin.uid = url
    return twin
end

function islistening(router::AbstractRouter; protocol::Vector{Symbol}=[:ws], wait=0)
    r = top_router(router)
    while wait >= 0
        all_listening = true
        for p in protocol
            if !haskey(r.listeners, p)
                return false
            elseif r.listeners[p].status === off
                all_listening = false
            end
        end
        sleep(0.2)
        all_listening && break
        wait -= 0.2
    end

    return (wait >= 0)
end

function islistening(twin::Twin; protocol::Vector{Symbol}=[:ws], wait=0)
    return islistening(twin.router; protocol=protocol, wait=wait)
end

function local_eval(router::Router, twin::Twin, msg::RembusMsg)
    result = nothing
    sts = STS_GENERIC_ERROR
    if isa(msg.data, Base.GenericIOBuffer)
        payload = dataframe_if_tagvalue(decode(copy(msg.data)))
    else
        payload = msg.data
    end

    try
        if router.shared === missing
            args = getargs(payload)
            if isa(args, Dict)
                kargs = Dict(Symbol(k) => v for (k, v) in args)
                result = router.local_function[msg.topic](; kargs...)
            else
                result = router.local_function[msg.topic](args...)
            end
        else
            args = getargs(payload)
            if isa(args, Dict)
                kargs = Dict{Symbol,Any}(Symbol(k) => v for (k, v) in args)
                kargs[:ctx] = router.shared
                kargs[:node] = twin
                result = router.local_function[msg.topic](; kargs...)
            else
                result = router.local_function[msg.topic](
                    args..., ctx=router.shared, node=twin
                )
            end
        end
        sts = STS_SUCCESS
    catch e
        # @debug "[$(msg.topic)] server error (method too young?): $e"
        result = "$e"
        sts = STS_METHOD_EXCEPTION

        if isa(e, MethodError)
            try
                result = Base.invokelatest(
                    router.local_function[msg.topic],
                    getargs(payload)...
                )
                sts = STS_SUCCESS
            catch e
                try
                    result = Base.invokelatest(
                        router.local_function[msg.topic],
                        getargs(payload)...,
                        ctx=router.shared,
                        node=twin,
                    )
                    sts = STS_SUCCESS
                catch e
                    result = "$e"
                end
            end
        end
    end
    if sts != STS_SUCCESS
        @error "[$(msg.topic)] local eval: $result"
    end

    if !haskey(router.local_subscriber, msg.topic)
        resmsg = ResMsg(msg, sts, result)
        @debug "[broker] response: $resmsg ($(resmsg.data))"
        respond(router, resmsg, twin)
    end

    return nothing
end

function glob_eval(router::Router, twin::Twin, msg::RembusMsg)
    result = nothing
    sts = STS_GENERIC_ERROR
    if isa(msg.data, Base.GenericIOBuffer)
        payload = dataframe_if_tagvalue(decode(msg.data))
    else
        payload = msg.data
    end
    try
        if router.shared === missing
            result = router.local_function["*"](msg.topic, getargs(payload)...)
        else
            result = router.local_function["*"](
                msg.topic, getargs(payload)..., ctx=router.shared, node=twin
            )
        end
        sts = STS_SUCCESS
    catch e
        @debug "[$(msg.topic)] server error (method too young?): $e"
        result = "$e"
        sts = STS_METHOD_EXCEPTION

        if isa(e, MethodError)
            try
                if router.shared === missing
                    result = Base.invokelatest(
                        router.local_function[msg.topic],
                        getargs(payload)...
                    )
                else
                    result = Base.invokelatest(
                        router.local_function[msg.topic],
                        getargs(payload)...,
                        ctx=router.shared,
                        node=twin
                    )
                end
                sts = STS_SUCCESS
            catch e
                result = "$e"
            end
        end
    end

    if sts != STS_SUCCESS
        @error "[$(msg.topic)] glob eval: $result"
    end

    return nothing
end

function local_fn(router::Router, twin::Twin, msg)
    if haskey(router.local_function, msg.topic)
        Threads.@spawn local_eval(router, twin, msg)
        return true
    end
    return false
end

function local_subscribers(router::Router, twin::Twin, msg::RembusMsg)
    if haskey(router.local_function, msg.topic)
        Threads.@spawn local_eval(router, twin, msg)
    elseif haskey(router.local_function, "*")
        Threads.@spawn glob_eval(router, twin, msg)
    end
    return nothing
end

function broker_isnamed(router::Router)
    idv4_reg = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    return !occursin(idv4_reg, router.id)
end


function init(router::Router)
    router.local_function["rid"] = (ctx=missing, twin=nothing) -> router.id
    router.local_function["uptime"] = (ctx=missing, twin=nothing) -> uptime(router)
    router.local_function["version"] = (ctx=missing, twin=nothing) -> Rembus.VERSION

    router.local_function["julia_service_install"] = (
        name,
        cnt,
        ctx=missing,
        twin=nothing
    ) -> service_install(router, name, cnt, ctx, twin)
    router.local_function["julia_service_uninstall"] = (
        name,
        ctx=missing,
        twin=nothing
    ) -> service_uninstall(router, name, ctx, twin)
    router.local_function["julia_subscriber_install"] = (
        name,
        cnt,
        ctx=missing,
        twin=nothing
    ) -> subscriber_install(router, name, cnt, ctx, twin)
    router.local_function["julia_subscriber_uninstall"] = (
        name,
        ctx=missing,
        twin=nothing
    ) -> subscriber_uninstall(router, name, ctx, twin)

end

function first_up(::Router, tenant, topic, implementors)
    target = nothing
    @debug "[$topic] first_up routing policy"
    for tw in implementors
        @debug "[$topic] candidate target: $tw"
        if isopen(tw.socket) && (domain(tw) == tenant)
            target = tw
            break
        end
    end

    return target
end

function round_robin(router::Router, tenant, topic, implementors)
    target = nothing
    if !isempty(implementors)
        len = length(implementors)
        @debug "[$topic]: $len implementors: $implementors"
        current_index = get(router.last_invoked, topic, 1)
        candidate = nothing
        candidate_index = 1
        for (idx, tw) in enumerate(implementors)
            if idx < current_index
                if isnothing(candidate) && isopen(tw) && (domain(tw) == tenant)
                    candidate = tw
                    candidate_index = idx
                end
            else
                if isopen(tw) && (domain(tw) == tenant)
                    target = tw
                    router.last_invoked[topic] = idx >= len ? 1 : idx + 1
                    break
                end
            end
        end
    end

    if isnothing(target) && !isnothing(candidate)
        target = candidate
        router.last_invoked[topic] = candidate_index >= len ? 1 : candidate_index + 1
    end
    return target
end

Base.min(t::Twin) = t

Base.isless(t1::Twin, t2::Twin) = length(t1.socket.out) < length(t2.socket.out)

function less_busy(router, tenant, topic, implementors)
    up_and_running = [t for t in implementors if isopen(t) && domain(t) == tenant]
    return isempty(up_and_running) ? nothing : min(up_and_running...)
end

#=
    broadcast_msg(router, msg)

Broadcast the `topic` data `msg` to all interested clients.
=#
function broadcast_msg(router::Router, msg::PubSubMsg)
    authtwins = Set{Twin}()
    # The interest * (subscribe to all topics) is enabled
    # only for pubsub messages and not for rpc methods.
    topic = msg.topic
    src_twin = msg.twin
    twins = get(router.topic_interests, "*", Set{Twin}())
    # Broadcast to twins that are admins and to twins that are authorized to
    # subscribe to topic.
    for twin in twins
        if rid(twin) in router.admins
            push!(authtwins, twin)
        elseif haskey(router.topic_auth, topic)
            if haskey(router.topic_auth[topic], rid(twin))
                # It is a private topic, check if twin is authorized.
                push!(authtwins, twin)
            end
        else
            # It is a public topic, all twins may be broadcasted.
            push!(authtwins, twin)
        end
    end

    union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))
    for tw in authtwins
        # Do not publish back to the receiver channel and to unreactive components.
        if tw !== src_twin && tw.reactive && domain(tw) == domain(src_twin)
            @debug "[$router] broadcasting $msg to $tw"
            put!(tw.process.inbox, msg)
        end
    end

    return nothing
end

function broadcast_msg(router::Router, msg::ResMsg)
    src_twin = msg.twin
    authtwins = Set{Twin}()
    if isdefined(msg, :reqdata)
        topic = msg.reqdata.topic
        msg = PubSubMsg(src_twin, topic, msg.reqdata.data)
        union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))
        for tw in authtwins
            # Do not publish back to the receiver channel and to unreactive components.
            if tw !== src_twin && tw.reactive && domain(tw) == domain(src_twin)
                @debug "[$router] broadcasting $msg to $tw"
                put!(tw.process.inbox, msg)
            end
        end
    end

    return nothing
end

#=
    select_twin(router, topic, implementors)

Return an online implementor ready to execute the method associated to the topic.
=#
function select_twin(router::Router, tenant::AbstractString, topic, implementors)
    target = nothing
    @debug "[$topic] broker policy: $(router.policy)"
    if router.policy === :first_up
        target = first_up(router, tenant, topic, implementors)
    elseif router.policy === :round_robin
        target = round_robin(router, tenant, topic, implementors)
    elseif router.policy === :less_busy
        target = less_busy(router, tenant, topic, implementors)
    end

    return target
end

#=
Send a ResMsg message back to requestor.
=#
function respond(router::Router, msg::ResMsg)
    if !sendto_origin(msg.twin, msg)
        put!(msg.twin.process.inbox, msg)
        if msg.status != STS_SUCCESS
            return
        end

        # broadcast to all interested twins
        broadcast_msg(router, msg)
    end

    return nothing
end

respond(::Router, msg::RembusMsg, twin) = put!(twin.process.inbox, msg)

function rpc_response(router::Router, msg)
    twin = msg.twin
    @debug "[$twin] rpc_response: $msg"
    if haskey(twin.socket.out, msg.id)
        request = twin.socket.out[msg.id].request
        if msg.status == STS_CHALLENGE
            return resend_attestate(router, twin, msg)
        elseif isnothing(request) || isa(request, Attestation)
            @debug "[$twin] attestation ok: $msg"
            put!(twin.socket.out[CONNECTION_ID].future, msg)
        else
            msg.twin = twin.socket.out[msg.id].request.twin
            msg.reqdata = request
            respond(router, msg)
            elapsed = time() - twin.socket.out[msg.id].sending_ts
            if router.metrics !== nothing
                @debug "[$twin] $(msg.reqdata.topic) elapsed=$elapsed secs"
                h = Prometheus.labels(router.metrics.rpc, (msg.reqdata.topic,))
                Prometheus.observe(h, elapsed)
            end
        end
        close(twin.socket.out[msg.id].timer)
        delete!(twin.socket.out, msg.id)
    elseif msg.status == STS_CHALLENGE
        resend_attestate(router, twin, msg)
    else
        @debug "[$twin] unexpected response: $msg"
    end
end

function auth_identity(router::Router, msg)
    twin = msg.twin
    url = RbURL(msg.cid)
    twin_id = rid(url)
    if if_authenticated(router, twin_id)
        # cid is registered, send the challenge
        response = challenge(router, twin, msg.id)
        transport_send(twin, response)
    else
        attestation(router, twin, msg, false)
    end
end

#=
    broker_task(self, router)

Rembus broker main task.
=#
function router_task(self, router::Router, implementor_rule)
    try
        for msg in self.inbox
            @debug "[$router:broker] recv: $msg"
            !isshutdown(msg) || break
            probe_add(msg, pktin)
            if isa(msg, PubSubMsg)
                pubsub_msg(router, msg) || continue
            elseif isa(msg, RpcReqMsg)
                rpc_request(router, msg, implementor_rule)
            elseif isa(msg, ResMsg)
                # A result from an exposer.
                sendto_origin(msg.twin, msg) || rpc_response(router, msg)
            elseif isa(msg, AdminReqMsg)
                admin_msg(router, msg)
            elseif isa(msg, IdentityMsg)
                twin = msg.twin
                url = RbURL(msg.cid)
                twin_id = rid(url)
                @debug "[$twin] auth identity: $msg"
                if haskey(twin.handler, "challenge")
                    @debug "[$twin] challenge active"
                    # Await Attestation
                    @async await_attestation(router, twin, twin.socket, msg)
                elseif isempty(msg.cid)
                    response = ResMsg(twin, msg.id, STS_GENERIC_ERROR, "empty cid")
                    transport_send(twin, response)
                elseif isconnected(router, twin_id)
                    if router.settings.overwrite_connection
                        # close the already connected node
                        close_twin(router.id_twin[twin_id])
                        auth_identity(router, msg)
                    else
                        @warn "[$(path(twin))] node with id [$twin_id] is already connected"
                        response = ResMsg(twin, msg.id, STS_GENERIC_ERROR, "already connected")
                        transport_send(twin, response)
                    end
                    continue
                else
                    auth_identity(router, msg)
                end
                ### callbacks(twin)
            elseif isa(msg, PingMsg)
                twin = msg.twin
                if (twin.uid.id != msg.cid)
                    # broker restarted
                    # start the authentication flow if cid is registered
                    @debug "lost connection to broker: restarting $(msg.cid)"
                    if key_file(router, msg.cid) !== nothing
                        # check if challenge was already sent
                        if !haskey(twin.handler, "challenge")
                            response = challenge(router, twin, msg.id)
                            transport_send(twin, response)
                        end
                    else
                        attestation(router, twin, msg, false)
                    end
                else
                    if isa(twin.socket, ZRouter)
                        pong(twin.socket, msg.id, id)
                    end
                end
            elseif isa(msg, Register)
                twin = msg.twin
                if !hasname(twin)
                    @debug "[$twin] registering"
                    response = register_node(router, msg)
                    transport_send(twin, response)
                end
            elseif isa(msg, Unregister)
                unregister_node(router, msg)
            elseif isa(msg, Attestation)
                if !hasname(msg.twin)
                    attestation(router, msg.twin, msg)
                end
            elseif isa(msg, Close)
                offline!(router, msg.twin)
            elseif isa(msg, AckMsg)
                ack_msg(msg)
            elseif isa(msg, Ack2Msg)
                # Remove from the cache of already received messages.
                remove_message(msg)
            end
        end
    finally
        if broker_isnamed(router) && isopen(router.con)
            save_configuration(router)
        end
    end
end

function broker_task(self, router::Router)
    router_task(self, router, always_true)
end

#=
    server_task(self, router)

Server main task.

A server does nor route messages between connected nodes.
=#
function server_task(self, router::Router)
    router_task(self, router, isrepl)
end

# Find an implementor.
function find_implementor(router::Router, msg)
    topic = msg.topic
    twin = msg.twin
    if haskey(router.topic_impls, topic)
        implementors = router.topic_impls[topic]
        num_impls = length(implementors)

        # remove the requestor from the list of implementors
        filter!(t -> t != msg.twin, implementors)
        target = select_twin(router, domain(twin), topic, implementors)
        @debug "[broker] exposer for $topic: [$target]"
        if target === nothing
            if num_impls != length(implementors)
                resmsg = ResMsg(
                    msg,
                    STS_METHOD_LOOPBACK,
                    "$topic: method loopback"
                )
            else
                resmsg = ResMsg(
                    msg,
                    STS_METHOD_UNAVAILABLE,
                    "$topic: method unavailable"
                )
            end
            put!(msg.twin.process.inbox, resmsg)
        elseif target !== nothing
            put!(target.process.inbox, msg)
        end
    else
        resmsg = ResMsg(
            msg,
            STS_METHOD_NOT_FOUND,
            "$topic: method unknown"
        )
        put!(msg.twin.process.inbox, resmsg)
    end
end

#=
Remove the twin from the router tables.
=#
function cleanup(twin::Twin, router::Router)
    # Remove from address2twin
    filter!(((k, v),) -> twin != v, router.address2twin)
    # Remove from topic_impls
    for topic in keys(router.topic_impls)
        delete!(router.topic_impls[topic], twin)
        if isempty(router.topic_impls[topic])
            delete!(router.topic_impls, topic)
        end
    end
    # Remove from topic_interests
    for topic in keys(router.topic_interests)
        delete!(router.topic_interests[topic], twin)
        if isempty(router.topic_interests[topic])
            delete!(router.topic_interests, topic)
        end
    end

    delete!(router.id_twin, rid(twin))
    return nothing
end

function serve_ws(td, router::Router, port, issecure=false)
    @debug "[serve_ws] starting"
    sslconfig = nothing
    try
        if issecure
            sslconfig = secure_config(router)
        end

        listener(td, port, router, sslconfig)
        listener_status!(router, :ws, on)
        for msg in td.inbox
            if isshutdown(msg)
                break
            end
        end
    finally
        @debug "[serve_ws] closed"
        listener_status!(router, :ws, off)
        setphase(td, :terminate)
        isdefined(router, :ws_server) && close(router.ws_server)
    end
end

function zmq_broker_read_task(router::Router)
    while true
        try
            zmq_broker_read(router)
        catch e
            if isa(e, ZMQ.StateError)
                rethrow()
            elseif isopen(router.zmqsocket)
                @error "[serve_zmq] error: $e"
            end
        end
    end
end

#=
Read a packet from a ZeroMQ Router socket.
=#
function zmq_broker_read(router::Router)
    pkt = zmq_message(router)
    id = pkt.identity

    if haskey(router.address2twin, id)
        twin = router.address2twin[id]
    else
        @debug "creating anonymous twin from identity $id ($(bytes2zid(id)))"
        twin = bind(
            router,
            RbURL(
                name=string(bytes2zid(id)),
                protocol=:zmq,
                port=router.listeners[:zmq].port
            )
        )
        twin.socket = ZRouter(router.zmqsocket, id)
        @debug "[anonymous] client bound to twin id [$twin]"
        router.address2twin[id] = twin
    end

    # TODO: remove 2nd argument
    msg::RembusMsg = zmq_parse(twin, pkt, true)
    put!(twin.router.process.inbox, msg)
end

function serve_zmq(pd, router::Router, port)
    @debug "[serve_zmq] starting"
    try
        router.zmqcontext = ZMQ.Context()
        router.zmqsocket = Socket(router.zmqcontext, ROUTER)
        ZMQ.bind(router.zmqsocket, "tcp://*:$port")
        router.listeners[:zmq].status = on
        @debug "$(pd.supervisor) listening at port zmq:$port"
        setphase(pd, :listen)

        @async zmq_broker_read_task(router)
        for msg in pd.inbox
            if isshutdown(msg)
                break
            end
        end
    finally
        router.listeners[:zmq].status = off
        setphase(pd, :terminate)
        ZMQ.close(router.zmqsocket)
        ZMQ.close(router.zmqcontext)
        @debug "[serve_zmq] closed"
    end
end

function serve_tcp(pd, router::Router, port, issecure=false)
    proto = "tcp"
    server = nothing
    try
        IP = "0.0.0.0"
        if issecure
            proto = "tls"
            sslconfig = secure_config(router)
        end

        server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), port))
        router.tcp_server = server
        router.listeners[:tcp].status = on

        @debug "$(pd.supervisor) listening at port $proto:$port"
        setphase(pd, :listen)

        @async while true
            sock = accept(server)
            if issecure
                ctx = MbedTLS.SSLContext()
                MbedTLS.setup!(ctx, sslconfig)
                MbedTLS.associate!(ctx, sock)
                MbedTLS.handshake(ctx)
                @async client_receiver(router, TLS(ctx))
            else
                @async client_receiver(router, TCP(sock))
            end
        end

        for msg in pd.inbox
            if isshutdown(msg)
                break
            end
        end
    finally
        @debug "[serve_tcp] closed"
        router.listeners[:tcp].status = off
        setphase(pd, :terminate)
        server !== nothing && close(server)
    end
end

function get_router(;
    tables=OrderedDict(),
    name=localcid(),
    datadir=nothing,
    dbpath=nothing,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    prometheus=nothing,
    authenticated=false,
    secure=false,
    policy="first_up",
    keyspace=true,
    tsk=broker_task,
)
    broker_process = from("$name.broker")
    if broker_process === nothing
        router = Router{Twin}(name, dbpath, datadir, nothing, missing, tables)
        if authenticated
            router.mode = Rembus.authenticated
        end
        set_policy(router, policy)
        bp = process("broker", tsk, args=(router,), force_interrupt_after=30.0)
        router.process = bp


        if any(!isnothing, (ws, tcp, zmq, http))
            boot(router)
        else
            router.con = FileStore()
        end

        load_configuration(router)

        init(router)

        start_broker(
            router,
            secure=secure,
            name=name,
            ws=ws, tcp=tcp, zmq=zmq, http=http, prometheus=prometheus,
        )

        if keyspace
            add_plugin(router, KeySpaceRouter())
        end
    else
        router = broker_process.args[1]
    end

    return router
end

"""
    start_broker(;
        wait=true,
        secure=nothing,
        ws=nothing,
        tcp=nothing,
        zmq=nothing,
        http=nothing,
        prometheus=nothing,
        name="broker",
        authenticated=false,
        reset=nothing,
        plugin=nothing,
        context=nothing
    )

Start the node.

Return immediately when `wait` is false, otherwise blocks until shutdown is requested.

Overwrite command line arguments if args is not empty.
"""
function start_broker(
    router;
    secure=false,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    prometheus=nothing,
    name="broker",
)
    if all(isequal(nothing), (ws, tcp, zmq, http))
        router.settings.archiver_interval = 0
    end
    router.archiver = process(
        save_message,
        args=(router,),
        restart=:transient,
        force_interrupt_after=30.0)

    twin_sv = supervisor("twins", terminateif=:shutdown)
    tasks = [
        router.archiver,
        router.process,
        twin_sv
    ]

    if prometheus !== nothing
        registry = Prometheus.CollectorRegistry()
        router.metrics = RembusMetrics(registry)
        RouterCollector(router, registry=registry)
        Prometheus.GCCollector(; registry=registry)
        Prometheus.ProcessCollector(; registry=registry)
        push!(tasks, process(prometheus_task, args=(prometheus, registry)))
    end

    if http !== nothing
        set_listener(router, :http, http)
        push!(
            tasks,
            process(
                serve_http,
                args=(router, http, secure),
                trace_exception=true,
                restart=:transient,
                force_interrupt_after=2.0,
            )
        )
    end

    if tcp !== nothing
        set_listener(router, :tcp, tcp)
        push!(
            tasks,
            process(
                serve_tcp,
                args=(router, tcp, secure),
                trace_exception=true,
                restart=:transient,
                debounce_time=2
            )
        )
    end

    if zmq !== nothing
        set_listener(router, :zmq, zmq)
        push!(
            tasks,
            process(
                serve_zmq,
                args=(router, zmq),
                trace_exception=true,
                restart=:transient,
                debounce_time=2)
        )
    end

    if ws !== nothing
        set_listener(router, :ws, ws)
        push!(
            tasks,
            process(
                serve_ws,
                args=(router, ws, secure),
                trace_exception=true,
                restart=:transient,
                stop_waiting_after=2.0)
        )
    end

    supervise(
        [supervisor(name, tasks, strategy=:one_for_one, intensity=2)],
        wait=false,
        intensity=2
    )
    return router
end

function set_listener(router::Router, proto, port)
    router.listeners[proto] = Listener(port)
end

function listener_status!(router::Router, proto, status::ListenerStatus)
    router.listeners[proto].status = status
end

function set_plugin(twin::Twin, plugin, context=missing)
    router = top_router(twin.router)
    router.plugin = plugin
    router.shared = context
end
