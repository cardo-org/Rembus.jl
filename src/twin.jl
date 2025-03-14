#=
Start the twin process and add to the router id_twin map.
=#
function start_twin(router::Router, twin::Twin)
    id = tid(twin)
    spec = process(id, twin_task, args=(twin,))
    startup(Visor.from_supervisor(router.process.supervisor, "twins"), spec)
    yield()
    router.id_twin[id] = twin
    #router.twin_initialize(router.shared, twin)
    return twin
end

#=
    zmq_ping(rb::Twin)

Send a ping message to check if the broker is online.

Required by ZeroMQ socket.
=#
function zmq_ping(twin::Twin)
    try
        @debug "[$twin] zmq ping"
        if iszmq(twin)
            router = last_downstream(twin.router)
            if isopen(twin.socket)
                send_msg(twin, PingMsg(twin, twin.uid.id)) |> fetch
            end
            if router.settings.zmq_ping_interval > 0
                Timer(tmr -> zmq_ping(twin), router.settings.zmq_ping_interval)
            end
        end
    catch e
        @warn "[$(cid(twin))]: pong not received ($e)"
        dumperror(e)
    end

    return nothing
end

function pkfile(name; create_dir=false)
    cfgdir = joinpath(rembus_dir(), name)
    if !isdir(cfgdir) && create_dir
        mkpath(cfgdir)
    end

    return joinpath(cfgdir, ".secret")
end

function resend_attestate(router::Router, twin::Twin, response)
    if !hasname(twin)
        return nothing
    end
    msg = attestate(router, twin, response)
    put!(twin.process.inbox, msg)
    if twin.uid.protocol == :zmq && router.settings.zmq_ping_interval > 0
        Timer(tmr -> zmq_ping(twin), router.settings.zmq_ping_interval)
    end

    return nothing
end

function sign(ctx::MbedTLS.PKContext, hash_alg::MbedTLS.MDKind, hash, rng)
    n = 1024 # MBEDTLS_MPI_MAX_SIZE defined in mbedtls bignum.h
    output = Vector{UInt8}(undef, n)
    len = MbedTLS.sign!(ctx, hash_alg, hash, output, rng)
    output[1:len]
end

#=
Create the Attestation message to be sent by the connecting node.
=#
function attestate(router::Router, twin::Twin, response)
    cid = twin.uid.id
    file = pkfile(cid)
    if !isfile(file)
        error("missing/invalid $cid secret")
    end

    meta = Dict(string(proto) => lner.port for (proto, lner) in router.listeners)
    try
        ctx = MbedTLS.parse_keyfile(file)
        plain = encode([Vector{UInt8}(response_data(response)), cid])
        hash = MbedTLS.digest(MD_SHA256, plain)
        signature = sign(ctx, MD_SHA256, hash, MersenneTwister(0))
        return Attestation(twin, cid, signature, meta)
    catch e
        if isa(e, MbedTLS.MbedException)
            # try with a plain secret
            secret = readline(file)
            plain = encode([Vector{UInt8}(response_data(response)), secret])
            hash = MbedTLS.digest(MD_SHA256, plain)
            return Attestation(twin, cid, hash, meta)
        end
    end
end

function await_challenge_timeout(twin)
    @debug "[$twin] await challenge timeout"
    if haskey(twin.socket.out, CONNECTION_ID)
        put!(twin.socket.out[CONNECTION_ID].future, RembusTimeout("inquiry timeout"))
        delete!(twin.socket.out, CONNECTION_ID)
    end
    return nothing
end

#=
A broker with ConnectionMode equal to authenticated send immediately
a challenge.
=#
function await_challenge(router::Router, twin::Twin)
    @debug "[$twin] awaiting challenge"
    t = Timer((tim) -> await_challenge_timeout(twin), router.settings.request_timeout)
    rf = FutureResponse(nothing, t)
    twin.socket.out[CONNECTION_ID] = rf
    response = fetch(rf.future)
    close(t)
    if isa(response, RembusTimeout)
        @warn "[$twin] no challenge from remote"
        throw(response)
    elseif response.status !== STS_SUCCESS
        @warn "[$twin] authentication failed"
        throw(rembuserror(code=response.status))
    end
    return nothing
end

function keep_alive(twin)
    router = last_downstream(twin.router)
    router.settings.ws_ping_interval == 0 && return
    try
        while true
            sleep(router.settings.ws_ping_interval)
            (isa(twin.socket, WS) &&
             isopen(twin.socket.sock.io)) ? WebSockets.ping(twin.socket.sock) : break
        end
    catch
    finally
        @debug "socket connection closed, keep alive done"
    end
end

acks_file(r::Router, id::AbstractString) = joinpath(rembus_dir(), r.id, "$id.acks")

#=
    load_pubsub_received(component::RbURL)

Load from file the ids of received Pub/Sub messages
awaiting Ack2 acknowledgements.
=#
function load_pubsub_received(router::Router, component::RbURL)
    if hasname(component)
        path = acks_file(router, component.id)
        if isfile(path)
            return load_object(path)
        end
    end
    return ack_dataframe()
end

#=
    save_pubsub_received(rb::RBHandle)

Save to file the ids of received Pub/Sub messages
waitings Ack2 acknowledgements.
=#
function save_pubsub_received(twin::Twin)
    router = last_downstream(twin.router)
    path = acks_file(router, twin.uid.id)
    save_object(path, twin.ackdf)
end

#=
    add_pubsub_id(twin, msg)

Add the message id for PubSub messages with QOS2 quality level to the set of
set of already received messages.
=#
function add_pubsub_id(twin, msg)
    push!(twin.ackdf, (UInt64(msg.id >> 64), msg.id))
end

function already_received(twin, msg)
    findfirst(==(msg.id), twin.ackdf.id) !== nothing
end

function remove_message(msg)
    twin = msg.twin
    idx = findfirst(==(msg.id), twin.ackdf.id)
    if idx !== nothing
        deleteat!(twin.ackdf, idx)
    end
end

function zmq_receive(twin::Twin)
    while true
        try
            msg = zmq_load(twin, twin.socket.sock)
            put!(twin.router.process.inbox, msg)
        catch e
            if isa(e, EOFError) && !isopen(twin.socket)
                # Assume that an EOFError is thrown only when a zmq socket
                # is explicitly closed.
                break
            else
                @error "[$twin] zmq_receive: $e"
                dumperror(e)
            end
        end
    end
    @debug "[$twin] zmq socket closed"
end

#=
Establish the connection from a component or from a broker's twin..
=#
function zmq_connect(rb)
    rb.socket = ZDealer()
    url = nodeurl(rb)
    ZMQ.connect(rb.socket.sock, url)
    @async zmq_receive(rb)
    return nothing
end

function tcp_connect(rb, isconnected::Condition)
    try
        url = nodeurl(rb)
        uri = URI(url)
        @debug "connecting to $(uri.scheme):$(uri.host):$(uri.port)"
        if uri.scheme == "tls"
            if haskey(ENV, "HTTP_CA_BUNDLE")
                cacert = ENV["HTTP_CA_BUNDLE"]
            else
                cacert = rembus_ca()
            end

            entropy = MbedTLS.Entropy()
            rng = MbedTLS.CtrDrbg()
            MbedTLS.seed!(rng, entropy)

            ctx = MbedTLS.SSLContext()

            sslconf = MbedTLS.SSLConfig(true)
            MbedTLS.config_defaults!(sslconf)

            MbedTLS.rng!(sslconf, rng)

            MbedTLS.ca_chain!(sslconf, MbedTLS.crt_parse(read(cacert, String)))

            function show_debug(level, filename, number, msg)
                println((level, filename, number, msg))
            end

            MbedTLS.dbg!(sslconf, show_debug)

            sock = Sockets.connect(uri.host, parse(Int, uri.port))
            MbedTLS.setup!(ctx, sslconf)
            MbedTLS.set_bio!(ctx, sock)
            MbedTLS.handshake(ctx)

            rb.socket = TLS(ctx)
            notify(isconnected)
            twin_receiver(rb)
        elseif uri.scheme == "tcp"
            sock = Sockets.connect(uri.host, parse(Int, uri.port))
            rb.socket = TCP(sock)
            notify(isconnected)
            twin_receiver(rb)
        else
            error("tcp endpoint: wrong $(uri.scheme) scheme")
        end
    catch e
        notify(isconnected, e, error=true)
    end
end

function ws_connect(rb::Twin, isconnected::Condition)
    try
        url = nodeurl(rb)
        uri = URI(url)

        if uri.scheme == "wss"

            if !haskey(ENV, "HTTP_CA_BUNDLE")
                ENV["HTTP_CA_BUNDLE"] = rembus_ca()
            end
            @debug "cacert: $(ENV["HTTP_CA_BUNDLE"])"
            HTTP.WebSockets.open(socket -> begin
                    rb.socket = WS(socket)
                    notify(isconnected)
                    @async keep_alive(rb)
                    twin_receiver(rb)
                end, url)
        else # uri.scheme == "ws"
            HTTP.WebSockets.open(socket -> begin
                    ## Sockets.nagle(socket.io.io, false)
                    ## Sockets.quickack(socket.io.io, true)
                    ### setup_receiver(process, socket, rb, isconnected)
                    ### read_socket(socket, rb, isconnected)
                    rb.socket = WS(socket)
                    notify(isconnected)
                    @async keep_alive(rb)
                    twin_receiver(rb)
                end, url, idle_timeout=1, forcenew=true)
        end
    catch e
        notify(isconnected, e, error=true)
        dumperror(e)
    end
end

#=
(Re)Connect to the remote endpoint.
=#
function transport_connect(rb::Twin)
    proto = protocol(rb)
    if proto === :ws || proto === :wss
        isconnected = Condition()
        @async ws_connect(rb, isconnected)
        wait(isconnected)
    elseif proto === :tcp || proto === :tls
        isconnected = Condition()
        @async tcp_connect(rb, isconnected)
        wait(isconnected)
    elseif proto === :zmq
        zmq_connect(rb)
    end

    return rb
end

"""
    component(urls::Vector)

Connect component to nodes defined be `urls` array.
"""
function component(
    urls::Vector;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up"
)
    if ismissing(name)
        nodeurl = cid()
        name = RbURL(nodeurl).id
    end

    router = get_router(
        name=name, ws=ws, tcp=tcp, zmq=zmq, authenticated=authenticated, secure=secure
    )
    set_policy(router, policy)
    for url in urls
        component(url, router)
    end
    return bind(router)
end

function singleton()
    if isempty(cid())
        cid!(string(uuid4()))
    end

    return component()
end

component() = component(cid())

function component(
    url::RbURL;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
)
    if ismissing(name)
        name = tid(url)
    end
    router = get_router(
        name=name, ws=ws, tcp=tcp, zmq=zmq, authenticated=authenticated, secure=secure
    )
    set_policy(router, policy)
    return component(url, router)
end

function component(
    url::AbstractString,
    router::AbstractRouter
)
    uid = RbURL(url)
    return component(uid, router)
end

function component(url::RbURL, router::AbstractRouter)
    twin = bind(router, url)
    down_handler = (twin) -> @async reconnect(twin)
    twin.handler[HR_CONN_DOWN] = down_handler
    try
        !do_connect(twin)
    catch e
        if isa(e, HTTP.Exceptions.ConnectError)
            @error "[$twin]: $(e.error.ex)"
        else
            @error "[$twin] connect: $e"
        end
        down_handler(twin)
    finally
    end

    return twin
end

function connect(url::RbURL; name=missing)
    if ismissing(name)
        name = url.id
    end

    router = get_router(name=name)
    twin = bind(router, url)
    try
        !do_connect(twin)
    catch
        shutdown(twin)
        rethrow()
    end
    return twin
end

function connect(url::AbstractString; name=missing)
    return connect(RbURL(url), name=name)
end

connect() = connect(RbURL())

function Visor.shutdown(twin::Twin)
    if isdefined(twin, :process)
        twin.process.phase = :closing
        shutdown(twin.process.supervisor.supervisor)
    end
end

Base.close(twin::Twin) = Visor.shutdown(twin)

function close_twin(twin::Twin)
    if isdefined(twin, :process)
        twin.process.phase = :closing
        shutdown(twin.process)
    end
    router = last_downstream(twin.router)
    delete!(router.id_twin, tid(twin))
    return nothing
end

#=
Return the CA certificate full path.

The full path is the concatenation of rembus_dir and the file present in
rembus_dir/ca
=#
function rembus_ca()
    dir = joinpath(rembus_dir(), "ca")
    if isdir(dir)
        files = readdir(dir)
        if length(files) == 1
            return joinpath(dir, files[1])
        end
    end

    throw(CABundleNotFound())
end

"""
    do_connect(twin::Twin)

Connect anonymously to the endpoint declared with `REMBUS_BASE_URL` env variable.

`REMBUS_BASE_URL` default to `ws://127.0.0.1:8000`

A component is considered anonymous when a different and random UUID is used as
component identifier each time the application connect to the broker.
"""
function do_connect(twin::Twin)
    if !isopen(twin.socket)
        router = last_downstream(twin.router)
        if router.settings.connection_mode === authenticated
            if !hasname(twin)
                error("anonymous components not allowed")
            end
            transport_connect(twin)
            await_challenge(router, twin)
        else
            transport_connect(twin)
            authenticate(router, twin)
        end
    end

    return isopen(twin.socket)
end

function update_tables(router::Router, twin::Twin, exports)
    if isnothing(exports)
        return nothing
    end

    @debug "[$twin] exports: $(exports[1])"
    for topic in exports[1]
        if !haskey(router.topic_impls, topic)
            router.topic_impls[topic] = OrderedSet{Twin}()
        end
        push!(router.topic_impls[topic], twin)
    end

    for topic in exports[2]
        if !haskey(router.topic_interests, topic)
            router.topic_interests[topic] = OrderedSet{Twin}()
        end
        push!(router.topic_interests[topic], twin)
    end

    return nothing
end

#=
The connecting node declare its identity to the broker and authenticate if prompted.
=#
function authenticate(router::Router, twin::Twin)
    if !hasname(twin) || isa(twin.socket, Float)
        return nothing
    end

    meta = Dict(string(proto) => lner.port for (proto, lner) in router.listeners)
    reason = nothing
    msg = IdentityMsg(twin, cid(twin), meta)
    response = twin_request(twin, msg, router.settings.request_timeout)
    @debug "[$twin] authenticate: $response"
    if (response.status == STS_GENERIC_ERROR)
        close(twin.socket)
        throw(AlreadyConnected(tid(twin)))
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(router, twin, response)
        response = twin_request(twin, msg, router.settings.request_timeout)
    end

    if (response.status != STS_SUCCESS)
        close(twin.socket)
        rembuserror(code=response.status, reason=reason)
    else
        update_tables(router, twin, response_data(response))
        if iszmq(twin)
            router.settings.zmq_ping_interval > 0 &&
                Timer(tmr -> zmq_ping(twin), router.settings.zmq_ping_interval)
        end
    end

    return nothing
end

#=
    setidentity(router, twin, msg; isauth=false)

Update twin identity parameters.
=#
function setidentity(router::Router, twin::Twin, msg; isauth=false)
    delete!(router.id_twin, tid(twin))
    twin.uid = RbURL(msg.cid)
    router.id_twin[tid(twin)] = twin
    setname(twin.process, tid(twin))
    twin.isauth = isauth
    load_twin(twin)
    return nothing
end

function verify_signature(router::Router, twin::Twin, msg)
    if !haskey(twin.handler, "challenge")
        error("[$twin] challenge not found")
    end

    fn = pop!(twin.handler, "challenge")
    challenge = fn(twin)
    @debug "verify signature, challenge $challenge"
    file = pubkey_file(router, msg.cid)
    try
        ctx = MbedTLS.parse_public_keyfile(file)
        plain = encode([challenge, msg.cid])
        hash = MbedTLS.digest(MD_SHA256, plain)
        MbedTLS.verify(ctx, MD_SHA256, hash, msg.signature)
    catch e
        if isa(e, MbedTLS.MbedException) &&
           e.ret == MbedTLS.MBEDTLS_ERR_RSA_VERIFY_FAILED
            rethrow()
        end
        # try with a plain secret
        @debug "verify signature with password string"
        secret = readline(file)
        plain = encode([challenge, secret])
        digest = MbedTLS.digest(MD_SHA256, plain)
        if digest != msg.signature
            error("authentication failed")
        end
    end

    return true
end

function login(router::Router, twin::Twin, msg)
    if isdefined(router.plugin, :login)
        login_fn = getfield(router.plugin, :login)
        login_fn(twin, msg.cid, msg.signature) || error("authentication failed")
    else
        verify_signature(router, twin, msg)
    end

    @debug "[$(msg.cid)] is authenticated"
    return nothing
end


function _topics(results, target::Twin, topic_map)
    @debug "[$target] calculating exported topics ..."
    for (topic, twins) in topic_map
        vals = filter(twins) do twin
            tid(twin) !== tid(target)
        end
        if !isempty(vals)
            push!(results, topic)
        end
    end

    # Convert to a list for cbor encoding optimization.
    return collect(results)
end

function topic_impls(router::Router, target::Twin)
    results = filter(keys(router.topic_function)) do topic
        !haskey(router.subinfo, topic)
    end

    return _topics(results, target, router.topic_impls)
end

function topic_interests(router::Router, target::Twin)
    results = filter(keys(router.topic_function)) do topic
        haskey(router.subinfo, topic)
    end

    return _topics(results, target, router.topic_interests)
end

function get_topics(r::Router, target::Twin)
    return [topic_impls(r, target), topic_interests(r, target)]
end

#=
    attestation(router, twin, msg, authenticate=true)

Authenticate the client.

If authentication fails then close the websocket.
=#
function attestation(router::Router, twin::Twin, msg, authenticate=true)
    @debug "[$twin] binding cid: $(msg.cid), authenticate: $authenticate"
    sts = STS_SUCCESS
    reason = nothing
    try
        if authenticate
            login(router, twin, msg)
        end

        setidentity(router, twin, msg, isauth=authenticate)
        reason = get_topics(router, twin)
        @debug "[$twin] exported topics: $reason"
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end
    transport_send(twin, ResMsg(twin, msg.id, sts, reason))

    if haskey(twin.handler, "att")
        twin.handler["att"](sts)
        delete!(twin.handler, "att")
    end

    if sts === STS_SUCCESS
        if isdefined(msg, :meta) && !isempty(msg.meta)
            push!(
                router.network,
                nodes(
                    tid(twin),
                    string(twin.socket.sock.io.peerip), msg.meta
                )...
            )
        end
    else
        # TODO: check if needed.
        sleep(0.5)
        detach(twin)
    end

    return nothing
end

function receiver_exception(twin, e)
    if haskey(twin.handler, HR_CONN_DOWN)
        twin.handler[HR_CONN_DOWN](twin)
    end
    if isconnectionerror(twin.socket, e)
        if close_is_ok(twin.socket, e)
            @debug "[$twin] connection closed"
        else
            @error "[$twin] connection closed: $e"
        end
    else
        @error "[$twin] receiver error: $e"
    end
end

function remove_twin(router::Router, twin::Twin)
    delete!(router.id_twin, tid(twin))
end

#=
    destroy_twin(twin, router)

Remove the twin from the system.

Shutdown the process and remove the twin from the router.
=#
function destroy_twin(twin::Twin, router::Router)
    detach(twin)
    if isdefined(twin, :process)
        Visor.shutdown(twin.process)
    end
    #delete!(router.id_twin, tid(twin))
    remove_twin(router, twin)
    return nothing
end

function end_receiver(twin::Twin)
    twin.reactive = false
    if hasname(twin)
        detach(twin)
    else
        destroy_twin(twin, last_downstream(twin.router))
    end
end

sendto_origin(::Twin, ::FutureResponse) = false # COV_EXCL_LINE

function sendto_origin(twin, msg)
    if haskey(twin.socket.direct, msg.id)
        put!(twin.socket.direct[msg.id].future, msg)
        close(twin.socket.direct[msg.id].timer)
        delete!(twin.socket.direct, msg.id)
        return true
    end
    return false
end

"""
    isauthenticated(twin::Twin)

Return true if the component is authenticated.
"""
isauthenticated(twin::Twin) = twin.isauth

router_isauthenticated(router::Router) = router.mode === authenticated

#=
Check if twin has the privilege to execute a command.
=#
function command_permitted(router::Router, twin::Twin)
    res = true
    if router_isauthenticated(router)
        res = isauthenticated(twin)
    end

    if !res
        @debug "[$twin]: [$msg] not authorized"
        close(twin.socket)
    end

    return res
end

function pubsub_msg(router::Router, msg)
    twin = msg.twin
    if !command_permitted(router, twin) || !isauthorized(router, twin, msg.topic)
        @warn "[$twin] is not authorized to publish on $(msg.topic)"
        return false
    else
        if msg.flags > QOS0
            put!(twin.process.inbox, AckMsg(twin, msg.id))
        end
        if msg.flags == QOS2
            if already_received(twin, msg)
                @info "[$twin] skipping already received message $msg"
                return true
            else
                add_pubsub_id(twin, msg)
            end
        end
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
    end

    return true
end

function ack_msg(msg)
    @debug "[$(msg.twin)] ack_msg: $msg"
    twin = msg.twin
    msgid = msg.id
    sock = twin.socket
    if haskey(sock.out, msgid)
        close(sock.out[msgid].timer)

        if sock.out[msgid].request.flags === QOS2
            # send the ACK2 message to the component
            put!(
                twin.process.inbox,
                Ack2Msg(twin, msgid)
            )
        end
        if !isready(sock.out[msgid].future)
            put!(sock.out[msgid].future, true)
        end
        delete!(sock.out, msgid)
    end

    return nothing
end

function admin_msg(router::Router, msg)
    twin = msg.twin

    if !command_permitted(router, twin)
        @debug "[$router] command $msg not permitted to [$twin]"
        return false
    end

    admin_res = admin_command(router, twin, msg)
    if isa(admin_res, EnableReactiveMsg)
        startup(
            router.process.supervisor,
            process(
                start_reactive,
                args=(twin, admin_res.msg_from),
                trace_exception=true,
                restart=:temporary
            )
        )
        response = ResMsg(twin, msg.id, STS_SUCCESS, nothing)
        put!(twin.process.inbox, response)
    else
        put!(twin.process.inbox, admin_res)
    end
    return true
end

function rpc_request(router::Router, msg, implementor_rule)
    twin = msg.twin
    if !command_permitted(router, twin) || !isauthorized(router, twin, msg.topic)
        m = ResMsg(msg, STS_GENERIC_ERROR, "unauthorized")
        put!(twin.process.inbox, m)
    elseif msg.target !== nothing
        # it is a direct rpc
        if haskey(router.id_twin, msg.target)
            target_twin = router.id_twin[msg.target]
            if !isopen(target_twin)
                m = ResMsg(msg, STS_TARGET_DOWN, msg.target)
                put!(twin.process.inbox, m)
            else
                if target_twin == twin
                    local_fn(router, twin, msg)
                else
                    # check if remote expose the topic
                    if haskey(router.topic_impls, msg.topic)
                        put!(target_twin.process.inbox, msg)
                    else
                        put!(
                            twin.process.inbox,
                            ResMsg(
                                msg, STS_METHOD_NOT_FOUND, "$(msg.topic): method unknown"
                            )
                        )
                    end
                end
            end
        else
            # target twin is unavailable
            m = ResMsg(msg, STS_TARGET_NOT_FOUND, msg.target)
            put!(twin.process.inbox, m)
        end
    else
        # msg is routable, get it to router
        @debug "[$twin] to router $(typeof(router)): $msg"
        if local_fn(router, twin, msg)
        elseif implementor_rule(twin.uid)
            find_implementor(router, msg)
        end
    end

    return msg
end

function messages_files(node, from_msg)
    allfiles = msg_files(node)
    nowts = time()
    mdir = Rembus.messages_dir(node)
    files = filter(allfiles) do fn
        if isa(node, Twin) && parse(Int, fn) <= node.mark
            # The message was already delivered when the
            # component was previously online.
            return false
        else
            ftime = mtime(joinpath(mdir, fn))
            delta = nowts - ftime
            if delta * 1_000_000 > from_msg
                @debug "skip $fn: mtime: $(unix2datetime(ftime)) ($delta secs from now)"
                return false
            end
        end
        return true
    end

    return files
end

function start_reactive(pd, twin::Twin, from_msg::Float64)
    twin.reactive = true
    @debug "[$twin] start reactive from: $(from_msg)"
    if hasname(twin) && (from_msg > 0.0)
        files = messages_files(twin, from_msg)
        for fn in files
            @debug "loading file [$fn]"
            from_disk_messages(twin, fn)
        end

        # send the cached in-memory messages
        from_memory_messages(twin)
    end

    return nothing
end

#=
function callbacks(twin::Twin)
    # Actually the broker does not declares to connecting nodes
    # the list of exposed and subscribed methods.
end
=#

#=
When broker mode is set equal to authenticated it may happen that
the broker sent an unsolicited challenge and the client at the same time
sent an Identity message. In this case the Identity response is delayed
until the original challenge is resolved with an Attestation.
=#
function await_attestation(router::Router, twin::Twin, socket, msg)
    future = Distributed.Future()
    t = Timer(router.settings.request_timeout) do _t
        put!(future, ResMsg(twin, msg.id, STS_GENERIC_ERROR, nothing))
    end

    twin.handler["att"] = (sts) -> put!(future, sts)
    sts = fetch(future)
    close(t)
    transport_send(socket, ResMsg(twin, msg.id, sts, nothing))
end

function challenge(router::Router, twin::Twin, msgid)
    #    if haskey(twin.handler, "challenge")
    #        challenge_val = twin.handler["challenge"](twin)
    #    else
    if isdefined(router.plugin, :challenge)
        challenge_fn = getfield(router.plugin, :challenge)
        challenge_val = challenge_fn(twin)
    else
        challenge_val = rand(RandomDevice(), UInt8, 4)
    end
    twin.handler["challenge"] = (twin) -> challenge_val
    return ResMsg(twin, msgid, STS_CHALLENGE, challenge_val)
end

#=
    twin_receiver(twin)

Receive messages from the client socket (ws or tcp).
=#
function twin_receiver(twin::Twin)
    @debug "[$twin] client is connected"
    try
        ws = twin.socket.sock
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.socket = FLOAT
                @debug "component [$twin]: connection closed"
                break
            end
            msg::RembusMsg = broker_parse(twin, payload)
            @debug "[$(path(twin))] twin_receiver << $msg"
            put!(twin.router.process.inbox, msg)
        end
    catch e
        receiver_exception(twin, e)
        dumperror(e)
    finally
        end_receiver(twin)
    end

    return nothing
end


function challenge_if_auth(router::Router, twin::Twin)
    if router.mode === authenticated
        chl = challenge(router, twin, CONNECTION_ID)
        transport_send(twin, chl)

        # Setup a timer for disconnecting the node if it is not authenticated meantime.
        t = Timer(router.settings.challenge_timeout) do t
            if !isauthenticated(twin)
                close_twin(twin)
            end
        end
    end
end

function if_authenticated(router::Router, twin_id)
    return key_file(router, twin_id) !== nothing || router.mode === authenticated
end

#=
Entry point of a new connection request from a node.
=#
function client_receiver(router::Router, socket)
    twin = bind(router, RbURL())
    twin.socket = socket
    @debug "[$twin] anonymous client connected"
    challenge_if_auth(router, twin)
    # ws/tcp socket receiver task
    twin_receiver(twin)
    return nothing
end

function ws_server!(router::Router, server)
    router.ws_server = server
end

function listener(proc, port, router::Router, sslconfig)
    IP = "0.0.0.0"
    server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), port))
    # router.ws_server = server
    ws_server!(router, server)
    proto = (sslconfig === nothing) ? "ws" : "wss"
    @info "$(proc.supervisor) listening at port $proto:$port"

    setphase(proc, :listen)

    return HTTP.WebSockets.listen!(
        IP,
        port,
        server=server,
        sslconfig=sslconfig,
        verbose=-1
    ) do ws
        client_receiver(router, WS(ws))
    end
end

#=
    detach(twin)

Disconnect the twin from the ws/tcp/zmq channel.
=#
function detach(twin)
    close(twin.socket)

    # Forward the message counter to the last message received when online
    # because these messages get already a chance to be delivered.
    if twin.reactive
        twin.mark = twin.router.mcounter
        twin.reactive = false
    end

    # Move the outstanding requests to the floating socket
    # This will trigger the requests timeout ...
    twin.socket = Float(twin.socket.out, twin.socket.direct)


    if !isempty(twin.ackdf)
        save_pubsub_received(twin)
    end

    return nothing
end

#=
    twin_task(self, twin)

Twin task that read messages from router and send them to client.

A twin enqueues the input messages when the component is offline.
=#
function twin_task(self, twin)
    try
        twin.process = self
        @debug "starting twin [$(tid(twin))]"
        for msg in self.inbox
            if isshutdown(msg)
                self.phase = :closing
                break
            else
                if !sendto_origin(twin, msg)
                    done = false
                    while !done && isopen(twin)
                        done = message_send(twin, msg)
                    end
                end
            end
        end
    finally
        detach(twin)
    end
    @debug "[$twin] task done"
end

function prometheus_task(self, port, registry)
    IP = "0.0.0.0"
    @info "starting prometheus at port $port"

    server = HTTP.listen!(IP, port) do http
        return Prometheus.expose(http, registry)
    end

    # wait for a message: the only one is a shutdown request.
    take!(self.inbox)
    close(server)
end
