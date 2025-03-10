struct ConnectionDown
    twin::Twin
end

function bind(router::Router, url=RbURL(protocol=:repl))
    twin = lock(router.lock) do
        if haskey(router.id_twin, tid(url))
            twin = router.id_twin[tid(url)]
        else
            twin = Twin(url, router)
        end

        if !isdefined(twin, :process) || istaskdone(twin.process.task)
            start_twin(twin)
        end

        return twin
    end
    twin.uid = url
    return twin
end

function router_ready(router)
    while isnan(router.start_ts)
        sleep(0.05)
    end
end

function islistening(router::Router; protocol::Vector{Symbol}=[:ws], wait=0)
    while wait >= 0
        all_listening = true
        for p in protocol
            if !haskey(router.listeners, p)
                return false
            elseif router.listeners[p].status === off
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

function local_eval(router, twin::Twin, msg::RembusMsg)
    result = nothing
    sts = STS_GENERIC_ERROR
    if isa(msg.data, Base.GenericIOBuffer)
        payload = dataframe_if_tagvalue(decode(msg.data))
    else
        payload = msg.data
    end

    try
        if router.shared === missing
            result = router.topic_function[msg.topic](getargs(payload)...)
        else
            result = router.topic_function[msg.topic](
                router.shared, twin, getargs(payload)...
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
                        router.topic_function[msg.topic],
                        getargs(payload)...
                    )
                else
                    result = Base.invokelatest(
                        router.topic_function[msg.topic],
                        router.shared,
                        twin,
                        getargs(payload)...
                    )
                end
                sts = STS_SUCCESS
            catch e
                result = "$e"
            end
        end
    end

    if sts != STS_SUCCESS
        @error "[$(msg.topic)] local eval: $result"
    end

    if !haskey(router.subinfo, msg.topic)
        resmsg = ResMsg(msg, sts, result)
        @debug "[broker] response: $resmsg ($(resmsg.data))"
        respond(router, resmsg, twin)
    end

    return nothing
end

function glob_eval(router, twin::Twin, msg::RembusMsg)
    result = nothing
    sts = STS_GENERIC_ERROR
    if isa(msg.data, Base.GenericIOBuffer)
        payload = dataframe_if_tagvalue(decode(msg.data))
    else
        payload = msg.data
    end
    try
        if router.shared === missing
            result = router.topic_function["*"](msg.topic, getargs(payload)...)
        else
            result = router.topic_function["*"](
                router.shared, twin, msg.topic, getargs(payload)...
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
                        router.topic_function[msg.topic],
                        getargs(payload)...
                    )
                else
                    result = Base.invokelatest(
                        router.topic_function[msg.topic],
                        router.shared,
                        twin,
                        getargs(payload)...
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

function local_fn(router, twin::Twin, msg)
    if haskey(router.topic_function, msg.topic)
        local_eval(router, twin, msg)
        return true
    end
    return false
end

function local_subscribers(router, twin::Twin, msg::RembusMsg)
    if haskey(router.topic_function, msg.topic)
        local_eval(router, twin, msg)
    elseif haskey(router.topic_function, "*")
        glob_eval(router, twin, msg)
    end
    return nothing
end

function broker_isnamed(router)
    idv4_reg = r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    return !occursin(idv4_reg, router.id)
end

#=
    boot(router)

Setup the router.
=#
function boot(router)
    if broker_isnamed(router)
        dir = broker_dir(router)
        if !isdir(dir)
            mkpath(dir)
        end

        appdir = keys_dir(router)
        if !isdir(appdir)
            mkdir(appdir)
        end

        msg_dir = messages_dir(router)
        if !isdir(msg_dir)
            mkdir(msg_dir)
        end

        load_configuration(router)
    end

    return nothing
end

function init(router)
    boot(router)
    router.topic_function["rid"] = (ctx=missing, twin=nothing) -> router.id
    router.topic_function["uptime"] = (ctx=missing, twin=nothing) -> uptime(router)
    router.topic_function["version"] = (ctx=missing, twin=nothing) -> Rembus.VERSION
end

function first_up(::Router, topic, implementors)
    target = nothing
    @debug "[$topic] first_up routing policy"
    for tw in implementors
        @debug "[$topic] candidate target: $tw"
        if isopen(tw.socket)
            target = tw
            break
        end
    end

    return target
end

function round_robin(router, topic, implementors)
    target = nothing
    if !isempty(implementors)
        len = length(implementors)
        @debug "[$topic]: $len implementors: $implementors"
        current_index = get(router.last_invoked, topic, 1)
        candidate = nothing
        candidate_index = 1
        for (idx, tw) in enumerate(implementors)
            if idx < current_index
                if isnothing(candidate) && isopen(tw)
                    candidate = tw
                    candidate_index = idx
                end
            else
                if isopen(tw)
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

function less_busy(router, topic, implementors)
    up_and_running = [impl for impl in implementors if isopen(impl)]
    return isempty(up_and_running) ? nothing : min(up_and_running...)
end

#=
    broadcast_msg(router, msg)

Broadcast the `topic` data `msg` to all interested clients.
=#
function broadcast_msg(router::Router, msg::RembusMsg)
    authtwins = Set{Twin}()
    if isa(msg, PubSubMsg)
        # The interest * (subscribe to all topics) is enabled
        # only for pubsub messages and not for rpc methods.
        topic = msg.topic
        twins = get(router.topic_interests, "*", Set{Twin}())
        # Broadcast to twins that are admins and to twins that are authorized to
        # subscribe to topic.
        for twin in twins
            if tid(twin) in router.admins
                push!(authtwins, twin)
            elseif haskey(router.topic_auth, topic)
                if haskey(router.topic_auth[topic], tid(twin))
                    # It is a private topic, check if twin is authorized.
                    push!(authtwins, twin)
                end
            else
                # It is a public topic, all twins may be broadcasted.
                push!(authtwins, twin)
            end
        end
    elseif isdefined(msg, :reqdata)
        topic = msg.reqdata.topic
        msg = PubSubMsg(msg.twin, topic, msg.reqdata.data)
        #    else
        #        @debug "no broadcast for [$msg]: request data not available or server method"
        #        return nothing
    end

    union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))
    for tw in authtwins
        # Do not publish back to the receiver channel and to unreactive components.
        if tw !== msg.twin || notreactive(tw)
            @debug "[$router] broadcasting $msg to $tw"
            put!(tw.process.inbox, msg)
        end
    end

    return nothing
end

#=
    select_twin(router, topic, implementors)

Return an online implementor ready to execute the method associated to the topic.
=#
function select_twin(router, topic, implementors)
    target = nothing
    @debug "[$topic] broker policy: $(router.policy)"
    if router.policy === :first_up
        target = first_up(router, topic, implementors)
    elseif router.policy === :round_robin
        target = round_robin(router, topic, implementors)
    elseif router.policy === :less_busy
        target = less_busy(router, topic, implementors)
    end

    return target
end


function reconnect(twin)
    twin.process.phase === :closing && return
    isdown = true
    while isdown
        sleep(2)
        try
            twin.process.phase === :closing && break
            isdown = !do_connect(twin)
        catch e
            @debug "[$twin] reconnecting..."
        end
    end
end

#=
function hosts_payload(hosts)
    res = [String[], UInt16[]]
    for host in hosts
        push!(res[1], string(host.host))
        push!(res[2], host.port)
    end
    return res
end

function leader_task(router::Router)
    while Visor.isrunning(router.process)
        sleep(2)
        for twin in values(router.id_twin)
            send_msg(
                twin,
                AdminReqMsg(
                    twin, name, Dict(COMMAND => LEADER_HERE, hosts_payload(router.network))
                )
            )
        end
    end
end
=#

#=
Send a ResMsg message back to requestor.
=#
function respond(router::Router, msg::ResMsg)
    put!(msg.twin.process.inbox, msg)

    if msg.status != STS_SUCCESS
        return
    end

    # broadcast to all interested twins
    broadcast_msg(router, msg)

    return nothing
end

respond(::Router, msg::RembusMsg, twin) = put!(twin.process.inbox, msg)

#=
    broker_task(self, router)

Rembus broker main task.
=#
function router_task(self, router, ready, implementor_rule)
    @debug "[broker] starting"

    try
        init(router)
        put!(ready, true)
        for msg in self.inbox
            @debug "[broker] recv: $msg"
            !isshutdown(msg) || break
            if isa(msg, PubSubMsg)
                if router.settings.save_messages
                    msg.counter = save_message(router, msg)
                end
                # Publish to interested twins.
                broadcast_msg(router, msg)
                # Relay to subscribers of this broker.
                local_subscribers(router, msg.twin, msg)
                if router.metrics !== nothing
                    counter = Prometheus.labels(router.metrics.pub, (msg.topic,))
                    Prometheus.inc(counter)
                end
            elseif isa(msg, RpcReqMsg)
                if local_fn(router, msg.twin, msg)
                elseif implementor_rule(msg.twin.uid)
                    find_implementor(router, msg)
                end
            elseif isa(msg, ResMsg)
                # A result from an exposer.
                respond(router, msg)
            elseif isa(msg, AdminReqMsg)
                admin_broadcast(router, msg.twin, msg)
            end
        end
        #    catch e
        #        @error "[broker] error: $e"
        #        rethrow()
    finally
        if broker_isnamed(router)
            save_configuration(router)
            if router.settings.save_messages
                persist_messages(router)
            end
            #filter!(router.id_twin) do (id, tw)
            #    cleanup(tw, router)
            #    return true
            #end
        end
        @debug "$(router.process.supervisor) shutted down"
    end
end

always_true(uid) = true

function broker_task(self, router, ready)
    router_task(self, router, ready, always_true)
end

#=
    server_task(self, router)

Server main task.

A server does nor route messages between connected nodes.
=#
function server_task(self, router, ready)
    router_task(self, router, ready, isrepl)
end

# Find an implementor.
function find_implementor(router, msg)
    topic = msg.topic
    if haskey(router.topic_impls, topic)
        implementors = router.topic_impls[topic]
        target = select_twin(router, topic, implementors)
        @debug "[broker] exposer for $topic: [$target]"
        if target === nothing
            resmsg = ResMsg(
                msg,
                STS_METHOD_UNAVAILABLE,
                "$topic: method unavailable"
            )
            put!(msg.twin.process.inbox, resmsg)
        elseif target === msg.twin
            @warn "[$target]: loopback detected"
            resmsg = ResMsg(
                msg,
                STS_METHOD_LOOPBACK,
                "$topic: method loopback"
            )
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

    delete!(router.id_twin, tid(twin))
    return nothing
end

function serve_ws(td, router, port, issecure=false)
    @debug "[serve_ws] starting"
    sslconfig = nothing
    try
        if issecure
            sslconfig = secure_config(router)
        end

        listener(td, port, router, sslconfig)
        router.listeners[:ws].status = on
        for msg in td.inbox
            if isshutdown(msg)
                break
            end
        end
    finally
        @debug "[serve_ws] closed"
        router.listeners[:ws].status = off
        setphase(td, :terminate)
        isdefined(router, :ws_server) && close(router.ws_server)
    end
end

function zmq_broker_read_task(router)
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
                hasname=false,
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
    target_twin = eval_message(twin, msg, id)
    if target_twin !== twin
        router.address2twin[id] = target_twin
    end
end

function serve_zmq(pd, router, port)
    @debug "[serve_zmq] starting"
    try
        router_ready(router)
        router.zmqcontext = ZMQ.Context()
        router.zmqsocket = Socket(router.zmqcontext, ROUTER)
        ZMQ.bind(router.zmqsocket, "tcp://*:$port")
        router.listeners[:zmq].status = on
        @info "$(pd.supervisor) listening at port zmq:$port"
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

function serve_tcp(pd, router, port, issecure=false)
    router_ready(router)
    proto = "tcp"
    server = nothing
    try
        IP = "0.0.0.0"
        if issecure
            proto = "tls"
            sslconfig = secure_config(router)
        end

        server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), port))
        router.server = server
        router.listeners[:tcp].status = on

        @info "$(pd.supervisor) listening at port $proto:$port"
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

const TRANSPORT_DISABLED::Int = 0
const TRANSPORT_WS::Int = 9000
const TRANSPORT_TCP::Int = 9001
const TRANSPORT_ZMQ::Int = 9002

function get_router(
    name,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    prometheus=nothing,
    authenticated=false,
    secure=false,
    tsk=broker_task
)
    process = from("$name.broker")
    if process === nothing
        router = start_broker(
            secure=secure,
            name=name,
            authenticated=authenticated,
            ws=ws, tcp=tcp, zmq=zmq, prometheus=prometheus,
            acceptor=tsk
        )
    else
        router = process.args[1]
    end

    return router
end

"""
    broker(
        url; name=missing, ws=nothing, tcp=nothing, zmq=nothing"
    )

Start a broker and return an handle for interacting with the network of nodes.
"""
function broker(;
    name::AbstractString="node",
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    prometheus=nothing,
    secure=false,
    authenticated=false
)
    router = get_router(name, ws, tcp, zmq, prometheus, authenticated, secure)
    # Return a floating twin.
    return bind(router)
end

function server(;
    name::AbstractString="node",
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    prometheus=nothing,
    authenticated=false,
    secure=false
)
    router = get_router(name, ws, tcp, zmq, prometheus, authenticated, secure, server_task)
    # Return a floating twin.
    return bind(router)
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
        policy=:first_up,
        authenticated=false,
        reset=nothing,
        plugin=nothing,
        context=nothing
    )

Start the node.

Return immediately when `wait` is false, otherwise blocks until shutdown is requested.

Overwrite command line arguments if args is not empty.
"""
function start_broker(;
    acceptor=broker_task,
    secure=false,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    prometheus=nothing,
    name="broker",
    authenticated=false,
    plugin=nothing,
    context=missing
)
    router = Router{Twin}(name, plugin, context)
    if authenticated
        router.mode = Rembus.authenticated
    end
    ready = Channel()
    bp = process("broker", acceptor, args=(router, ready))
    tasks = [
        bp,
        supervisor("twins", terminateif=:shutdown)
    ]
    router.process = bp

    if prometheus !== nothing
        registry = Prometheus.CollectorRegistry()
        router.metrics = RembusMetrics(registry)
        RouterCollector(router, registry=registry)
        Prometheus.GCCollector(; registry=registry)
        Prometheus.ProcessCollector(; registry=registry)
        push!(tasks, process(prometheus_task, args=(prometheus, registry)))
    end

    if http !== nothing
        router.listeners[:http] = Listener(http)
        push!(
            tasks,
            process(
                serve_http,
                args=(router, http, secure),
                trace_exception=true,
                restart=:transient
            )
        )
    end

    if tcp !== nothing
        router.listeners[:tcp] = Listener(tcp)
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
        router.listeners[:zmq] = Listener(zmq)
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
        router.listeners[:ws] = Listener(ws)
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
    # Wait that broker task is up and running.
    take!(ready)
    return router
end

function set_plugin(twin::Twin, plugin, context=missing)
    router = twin.router
    router.plugin = plugin
    router.shared = context
end
