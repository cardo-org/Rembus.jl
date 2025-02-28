function start_twin(twin::Twin)
    router = twin.router
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
        @debug "[$twin] zmq ping ($(typeof(twin.socket)))"
        if iszmq(twin)
            if isopen(twin.socket)
                send_msg(twin, PingMsg(twin.uid.id)) |> fetch
            end
            if twin.router.settings.zmq_ping_interval > 0
                Timer(tmr -> zmq_ping(twin), twin.router.settings.zmq_ping_interval)
            end
        end
    catch e
        @warn "[$(cid(twin))]: pong not received ($e)"
        dumperror(twin.router, e)
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

function resend_attestate(twin::Twin, response)
    if !hasname(twin)
        return nothing
    end
    msg = attestate(twin, response)
    put!(twin.process.inbox, msg)
    if twin.uid.protocol == :zmq && twin.router.settings.zmq_ping_interval > 0
        Timer(tmr -> zmq_ping(twin), twin.router.settings.zmq_ping_interval)
    end

    return nothing
end

function sign(ctx::MbedTLS.PKContext, hash_alg::MbedTLS.MDKind, hash, rng)
    n = 1024 # MBEDTLS_MPI_MAX_SIZE defined in mbedtls bignum.h
    output = Vector{UInt8}(undef, n)
    len = MbedTLS.sign!(ctx, hash_alg, hash, output, rng)
    output[1:len]
end

function attestate(twin, response)
    cid = twin.uid.id
    file = pkfile(cid)
    if !isfile(file)
        error("missing/invalid $cid secret")
    end

    meta = Dict(string(proto) => lner.port for (proto, lner) in twin.router.listeners)
    try
        ctx = MbedTLS.parse_keyfile(file)
        plain = encode([Vector{UInt8}(response_data(response)), cid])
        hash = MbedTLS.digest(MD_SHA256, plain)
        signature = sign(ctx, MD_SHA256, hash, MersenneTwister(0))
        return Attestation(cid, signature, meta)
    catch e
        if isa(e, MbedTLS.MbedException)
            # try with a plain secret
            secret = readline(file)
            plain = encode([Vector{UInt8}(response_data(response)), secret])
            hash = MbedTLS.digest(MD_SHA256, plain)
            return Attestation(cid, hash, meta)
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
function await_challenge(twin::Twin)
    @debug "[$twin] awaiting challenge"
    t = Timer((tim) -> await_challenge_timeout(twin), twin.router.settings.request_timeout)
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
    twin.router.settings.ws_ping_interval == 0 && return
    try
        while true
            sleep(twin.router.settings.ws_ping_interval)
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
    path = acks_file(twin.router, twin.uid.id)
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

function remove_message(twin, msg)
    idx = findfirst(==(msg.id), twin.ackdf.id)
    if idx !== nothing
        deleteat!(twin.ackdf, idx)
    end
end

function zmq_receive(twin::Twin)
    while true
        try
            msg = zmq_load(twin, twin.socket.sock)
            twin = eval_message(twin, msg)
        catch e
            if isa(e, EOFError) && !isopen(twin.socket)
                # Assume that an EOFError is thrown only when a zmq socket
                # is explicitly closed.
                break
            else
                @error "[$twin] zmq_receive: $e"
                dumperror(twin.router, e)
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
        dumperror(twin.router, e)
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

Connect component to remotes defined be `urls` array.

The connection pool is supervised.
"""
function component(
    urls::Vector;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    policy=:first_up,
    secure=false
)
    if ismissing(name)
        nodeurl = cid()
        name = RbURL(nodeurl).id
    end

    router = get_router(ws, tcp, zmq, nothing, false, name, secure)
    set_policy(router, policy)
    for url in urls
        component(url, router)
    end
    return bind(router)
end

component() = component(cid())

function component(
    url::AbstractString;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing
)
    uid = RbURL(url)
    return component(uid, ws=ws, tcp=tcp, zmq=zmq, name=name)
end

function component(
    url::RbURL;
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing
)
    if ismissing(name)
        name = tid(url)
    end
    router = start_broker(name=name, ws=ws, tcp=tcp, zmq=zmq)
    return component(url, router)
end

function component(
    url::AbstractString,
    router::Router
)
    uid = RbURL(url)
    return component(uid, router)
end

function component(url::RbURL, router::Router)
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

    router = start_broker(name=name)
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

    delete!(twin.router.id_twin, tid(twin))
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
    do_connect()

Connect anonymously to the endpoint declared with `REMBUS_BASE_URL` env variable.

`REMBUS_BASE_URL` default to `ws://127.0.0.1:8000`

A component is considered anonymous when a different and random UUID is used as
component identifier each time the application connect to the broker.
"""
function do_connect(twin::Twin)
    @debug "[$twin] connect mode: $(twin.router.settings.connection_mode)"
    if !isopen(twin.socket)
        if twin.router.settings.connection_mode === authenticated
            if !hasname(twin)
                error("anonymous components not allowed")
            end
            transport_connect(twin)
            await_challenge(twin)
        else
            transport_connect(twin)
            authenticate(twin)
        end
    end

    return isopen(twin.socket)
end

function update_tables(twin::Twin, exports)
    router = twin.router
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

function authenticate(twin)
    if !hasname(twin) || isa(twin.socket, Float)
        return nothing
    end

    meta = Dict(string(proto) => lner.port for (proto, lner) in twin.router.listeners)
    reason = nothing
    msg = IdentityMsg(twin, cid(twin), meta)
    response = twin_request(twin, msg, twin.router.settings.request_timeout)
    @debug "[$twin] authenticate: $response"
    if (response.status == STS_GENERIC_ERROR)
        close(twin.socket)
        throw(AlreadyConnected(tid(twin)))
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(twin, response)
        response = twin_request(twin, msg, twin.router.settings.request_timeout)
    end

    if (response.status != STS_SUCCESS)
        close(twin.socket)
        rembuserror(code=response.status, reason=reason)
    else
        update_tables(twin, response_data(response))
        if iszmq(twin)
            twin.router.settings.zmq_ping_interval > 0 &&
                Timer(tmr -> zmq_ping(twin), twin.router.settings.zmq_ping_interval)
        end
    end

    return nothing
end

#=
    setidentity(router, twin, msg; isauth=false)

Update twin identity parameters.
=#
function setidentity(router, twin, msg; isauth=false)
    uid = RbURL(msg.cid)
    if haskey(router.id_twin, tid(uid))
        namedtwin = router.id_twin[tid(uid)]
    else
        namedtwin = Twin(uid, router)
        start_twin(namedtwin)
    end

    namedtwin.socket = twin.socket
    namedtwin.handler = twin.handler
    namedtwin.isauth = isauth
    twin.socket = Float()
    destroy_twin(twin, router)
    return namedtwin
end

function verify_signature(twin, msg)
    if !haskey(twin.handler, "challenge")
        error("[$twin] challenge not found")
    end

    fn = pop!(twin.handler, "challenge")
    challenge = fn(twin)
    @debug "verify signature, challenge $challenge"
    file = pubkey_file(twin.router, msg.cid)
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

function login(router, twin, msg)
    if isdefined(router.plugin, :login)
        login_fn = getfield(router.plugin, :login)
        login_fn(twin, msg.cid, msg.signature) || error("authentication failed")
    else
        verify_signature(twin, msg)
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
function attestation(router, twin, msg, authenticate=true)
    @debug "[$twin] binding cid: $(msg.cid), authenticate: $authenticate"
    sts = STS_SUCCESS
    reason = nothing
    try
        if authenticate
            login(router, twin, msg)
        end

        twin = setidentity(router, twin, msg, isauth=authenticate)
        reason = get_topics(router, twin)
        @debug "[$twin] exported topics: $reason"
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end
    transport_send(twin, ResMsg(msg.id, sts, reason))

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
        detach(twin)
    end

    return twin
end

function receiver_exception(router, twin, e)
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

#=
    destroy_twin(twin, router)

Remove the twin from the system.

Shutdown the process and remove the twin from the router.
=#
function destroy_twin(twin, router)
    detach(twin)
    if isdefined(twin, :process)
        Visor.shutdown(twin.process)
    end
    delete!(router.id_twin, tid(twin))
    return nothing
end

function end_receiver(twin)
    twin.reactive = false
    if hasname(twin)
        detach(twin)
    else
        destroy_twin(twin, twin.router)
    end
end

sendto_origin(::Twin, ::FutureResponse) = false

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

function commands_permitted(twin)
    if twin.router.mode === authenticated
        return isauthenticated(twin)
    end
    return true
end

function pubsub_msg(router, twin, msg)
    if !isauthorized(router, twin, msg.topic)
        @warn "[$twin] is not authorized to publish on $(msg.topic)"
    else
        if msg.flags > QOS0
            put!(twin.process.inbox, AckMsg(msg.id))
        end
        if msg.flags == QOS2
            if already_received(twin, msg)
                @info "[$twin] skipping already received message $msg"
                return nothing
            else
                add_pubsub_id(twin, msg)
            end
        end
        put!(router.process.inbox, msg)
    end
    return nothing
end

function ack_msg(twin::Twin, msg)
    @debug "[$twin] ack_msg: $msg"
    msgid = msg.id
    sock = twin.socket
    if haskey(sock.out, msgid)
        close(sock.out[msgid].timer)

        if sock.out[msgid].request.flags === QOS2
            # send the ACK2 message to the component
            put!(
                twin.process.inbox,
                Ack2Msg(msgid)
            )
        end
        if !isready(sock.out[msgid].future)
            put!(sock.out[msgid].future, true)
        end
        delete!(sock.out, msgid)
    end

    return nothing
end

function admin_msg(router, twin, msg)
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
        response = ResMsg(msg.id, STS_SUCCESS, nothing)
        put!(twin.process.inbox, response)
    else
        put!(twin.process.inbox, admin_res)
    end
    return nothing
end

function rpc_request(router, twin, msg::RpcReqMsg)
    return _rpc_request(router, twin, msg, TYPE_RPC)
end

function _rpc_request(router, twin, msg, mtype)

    if !isauthorized(router, twin, msg.topic)
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
                if target_twin === twin
                    put!(router.process.inbox, msg)
                else
                    put!(target_twin.process.inbox, msg)
                end
            end
        else
            # target twin is unavailable
            m = ResMsg(msg, STS_TARGET_NOT_FOUND, msg.target)
            put!(twin.process.inbox, m)
        end
    else
        # msg is routable, get it to router
        @debug "[$twin] to router: $msg"
        put!(router.process.inbox, msg)
    end

    return msg
end

function rpc_response(router, twin, msg)
    @debug "[$twin] rpc_response: $msg"
    if haskey(twin.socket.out, msg.id)
        request = twin.socket.out[msg.id].request
        if msg.status == STS_CHALLENGE
            return resend_attestate(twin, msg)
        elseif isnothing(request) || isa(request, Attestation)
            @debug "[$twin] attestation ok: $msg"
            put!(twin.socket.out[CONNECTION_ID].future, msg)
        else
            msg.twin = twin.socket.out[msg.id].request.twin
            msg.reqdata = request
            put!(router.process.inbox, msg)
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
        resend_attestate(twin, msg)
    else
        @debug "[$twin] unexpected response: $msg"
    end
end

function command(router::Router, twin, msg)
    if commands_permitted(twin)
        if isa(msg, Unregister)
            unregister_node(router, twin, msg)
        elseif isa(msg, AdminReqMsg)
            admin_msg(router, twin, msg)
        elseif isa(msg, RpcReqMsg)
            rpc_request(router, twin, msg)
        elseif isa(msg, PubSubMsg)
            pubsub_msg(router, twin, msg)
        elseif isa(msg, AckMsg)
            ack_msg(twin, msg)
        elseif isa(msg, Ack2Msg)
            # Remove from the cache of already received messages.
            remove_message(twin, msg)
        end
    else
        @debug "[$twin]: [$msg] not authorized"
        close(twin.socket)
    end
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
function await_attestation(twin, socket, msg)
    future = Distributed.Future()
    t = Timer(twin.router.settings.request_timeout) do _t
        put!(future, ResMsg(msg.id, STS_GENERIC_ERROR, nothing))
    end

    twin.handler["att"] = (sts) -> put!(future, sts)
    sts = fetch(future)
    close(t)
    transport_send(socket, ResMsg(msg.id, sts, nothing))
end

function challenge(router, twin, msgid)
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
    return ResMsg(msgid, STS_CHALLENGE, challenge_val)
end

#=
Parse the message and invoke the actions related to the message type.
=#
function eval_message(twin, msg, id=UInt8[])
    target_twin = twin
    twin.probe && probe_add(twin, msg, pktin)
    router = twin.router
    @debug "[$twin] eval_message: $msg"
    if isa(msg, IdentityMsg)
        url = RbURL(msg.cid)
        twin_id = tid(url)
        @debug "[$twin] auth identity: $msg"
        if haskey(twin.handler, "challenge")
            @debug "[$twin] challenge active"
            # Await Attestation
            @async await_attestation(twin, twin.socket, msg)
        elseif isempty(msg.cid)
            response = ResMsg(msg.id, STS_GENERIC_ERROR, "empty cid")
            transport_send(twin, response)
        elseif haskey(router.id_twin, twin_id) && isconnected(router.id_twin[twin_id])
            @warn "[$(path(twin))] a component with id [$twin_id] is already connected"
            response = ResMsg(msg.id, STS_GENERIC_ERROR, "already connected")
            transport_send(twin, response)
        elseif key_file(router, twin_id) !== nothing || router.mode === authenticated
            # cid is registered, send the challenge
            # TODO: save wsport into session
            response = challenge(router, twin, msg.id)
            transport_send(twin, response)
        else
            target_twin = attestation(router, twin, msg, false)
        end
        #@mlog("[ZMQ][$twin] -> $response")
        ### callbacks(twin)
    elseif isa(msg, PingMsg)
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
                target_twin = attestation(router, twin, msg, false)
            end
        else
            if isa(twin.socket, ZRouter)
                pong(twin.socket, msg.id, id)
            end
        end
    elseif isa(msg, Register)
        if !hasname(twin)
            @debug "[$twin] registering"
            response = register_node(router, msg)
            transport_send(twin, response)
        end
    elseif isa(msg, Attestation)
        if !hasname(twin)
            target_twin = attestation(router, twin, msg)
        end
    elseif isa(msg, Close)
        offline!(twin)
    elseif isa(msg, ResMsg)
        sendto_origin(twin, msg) || rpc_response(router, twin, msg)
    else
        command(router, twin, msg)
    end

    return target_twin
end

#=
    twin_receiver(twin)

Receive messages from the client socket (ws or tcp).
=#
function twin_receiver(twin)
    router = twin.router
    @debug "[$twin] anonymous client is connected"
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
            target_twin = eval_message(twin, msg)
            if target_twin !== twin
                return target_twin
            end
        end
    catch e
        receiver_exception(router, twin, e)
        dumperror(twin.router, e)
    finally
        end_receiver(twin)
    end

    return nothing
end


#=
Entry point of a new connection request from a node.
=#
function client_receiver(router::Router, socket)
    twin = bind(router, RbURL())
    twin.socket = socket
    @debug "[$twin] anonymous client connected"
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
    # ws/tcp socket receiver task
    target_twin = twin_receiver(twin)
    if !isnothing(target_twin)
        twin_receiver(target_twin)
    end

    return nothing
end

function listener(proc, port, router, sslconfig)
    IP = "0.0.0.0"
    server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), port))
    router.ws_server = server
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
