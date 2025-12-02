#=
Start the twin process and add to the router id_twin map.
=#
function start_twin(router::Router, twin::Twin)
    id = rid(twin)
    spec = process(id, twin_task, args=(twin,), force_interrupt_after=30.0)
    twin.process = spec
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
        dumperror(twin, e)
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
        # disable reconnection, error is unrecoverable
        delete!(twin.handler, HR_CONN_DOWN)
        twin.process.phase = :closing
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
    while true
        sleep(router.settings.ws_ping_interval)
        put!(twin.process.inbox, WsPing())
    end
end

acks_file(r::Router, id::AbstractString) = joinpath(rembus_dir(), r.id, "$id.acks")

#=
    load_received_acks(router::Router, component::RbURL, ::FileStore)

Load from file the ids of received acks of Pub/Sub messages
awaiting Ack2 acknowledgements.
=#
function load_received_acks(router::Router, component::RbURL, ::FileStore)
    if hasname(component)
        path = acks_file(router, component.id)
        if isfile(path)
            return load_object(path)
        end
    end
    return ack_dataframe()
end

#=
    save_received_acks(rb::RBHandle, ::FileStore)

Save to file the ids of received acks of Pub/Sub messages
waitings Ack2 acknowledgements.
=#
function save_received_acks(twin::Twin, ::FileStore)
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
            if isa(e, EOFError) || isa(e, ZMQ.StateError)
                # Assume that an EOFError is thrown only when a zmq socket
                # is explicitly closed.
                break
            else
                @error "[$twin] zmq_receive $(typeof(e)): $e"
                dumperror(twin, e)
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

# Add missing hostname setter for MbedTLS.jl
function mbedtls_set_hostname!(ctx::MbedTLS.SSLContext, hostname::AbstractString)
    ptr = hasfield(typeof(ctx), :ctx) ? getfield(ctx, :ctx) : getfield(ctx, :data)
    ccall((:mbedtls_ssl_set_hostname, MbedTLS.libmbedtls), Cint,
        (Ptr{Cvoid}, Cstring), ptr, hostname)
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
            mbedtls_set_hostname!(ctx, uri.host)
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
        dumperror(twin, e)
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
    elseif proto === :mqtt
        connect(rb, Adapter(:MQTT))
    end

    return rb
end

"""
    component(urls::Vector)

Start a component that connects to a pool of nodes defined by the `urls` array.
"""
function component(
    urls::Vector,
    db=FileStore();
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR
)
    if ismissing(name)
        nodeurl = localcid()
        name = RbURL(nodeurl).id
    end

    router = get_router(
        db, name=name, ws=ws, tcp=tcp, zmq=zmq, authenticated=authenticated, secure=secure
    )
    set_policy(router, policy)
    for url_str in urls
        url = RbURL(url_str)
        url.props["pool"] = true
        c = component(url, router, enc)
        # a local twin of a pool component must be reactive to forward pub/sub messages
        c.reactive = true
    end
    return bind(router)
end

function singleton()
    if isempty(localcid())
        localcid!(string(uuid4()))
    end

    return component(localcid())
end

function component(
    url::RbURL,
    db=FileStore();
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR,
    failovers=[]
)
    # check for loopbacks
    if (url.host == "127.0.0.1" || url.host in string.(getipaddrs())) &&
       url.port in [ws, tcp, zmq, http]
        error("detected loopback component connection on port $(url.port)")
    end

    if ismissing(name)
        name = rid(url)
    end
    router = get_router(
        db,
        name=name,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        http=http,
        authenticated=authenticated,
        secure=secure
    )
    set_policy(router, policy)
    return component(url, router, enc, failovers)
end

function component(url::RbURL, router::AbstractRouter, enc=CBOR, failovers=[])
    if router.settings.connection_mode === authenticated && !hasname(url)
        error("anonymous components not allowed")
    end

    twin = bind(router, url)
    twin.enc = enc
    return add_failovers(twin, failovers)
end

function add_failovers(twin::Twin, failovers)
    twin.failovers = [twin.uid]
    for failover in failovers
        cid = RbURL(failover)
        cid.id = twin.uid.id
        push!(twin.failovers, cid)
    end

    down_handler = (twin) -> @async reconnect(twin)
    twin.handler[HR_CONN_DOWN] = down_handler
    try
        !do_connect(twin)
    catch e
        @error "[$twin]: $(isa(e, HTTP.Exceptions.ConnectError) ? e.error.ex : e)"
        down_handler(twin)
    finally
    end

    return twin
end

function connect(url::RbURL; name=missing, enc=CBOR)
    if ismissing(name)
        name = url.id
    end

    router = get_router(name=name)
    twin = bind(router, url)
    twin.enc = enc
    try
        !do_connect(twin)
    catch
        shutdown(twin)
        rethrow()
    end
    return twin
end

connect(enc=CBOR) = connect(RbURL(), enc=enc)

function disconnect(twin::Twin)
    if isdefined(twin, :process)
        twin.process.phase = :closing
        shutdown(twin.process)
    end
end

"""
$(TYPEDSIGNATURES)
Close the connection and terminate the component.
"""
function Visor.shutdown(rb::Twin)
    if isdefined(rb, :process)
        rb.process.phase = :closing
        shutdown(rb.process.supervisor.supervisor)
    end
end

"""
$(TYPEDSIGNATURES)
Close the connection and terminate the component.
"""
Base.close(rb::Twin) = Visor.shutdown(rb)

function close_twin(twin::Twin)
    if isdefined(twin, :process)
        twin.process.phase = :closing
        shutdown(twin.process)
    end
    router = last_downstream(twin.router)
    delete!(router.id_twin, rid(twin))
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

function reconnect(twin::Twin, url::RbURL)
    twin.uid = url
    isconnected = false
    try
        if do_connect(twin)
            router = last_downstream(twin.router)

            # repost the configuration
            @debug "[$twin] reposting exposed and subscribed"

            if !twin_setup(router, twin)
                @warn "[$twin] reconnection setup failed"
                disconnect(twin)
            else
                twin.process.phase = :running
                if !isempty(router.listeners)
                    # It may be a failover node. Close all connected nodes to force
                    # the reconnections to the main broker.
                    connected = filter(router.id_twin) do (id, t)
                        id !== rid(twin)
                    end
                    @debug "closing connected nodes"
                    for (id, tw) in connected
                        disconnect(tw)
                    end
                end

                isconnected = true
                send_data_at_rest(twin, twin.failover_from, twin.router.store)
            end
        end
    catch e
        @debug "[$twin] reconnecting error: ($e)"
    end

    return isconnected
end

function reconnect(twin::Twin)
    twin.process.phase === :closing && return
    @debug "[$twin] reconnecting..."
    period = last_downstream(twin.router).settings.reconnect_period
    while true
        for url in twin.failovers
            sleep(period)
            if twin.process.phase === :closing || reconnect(twin, url)
                return
            end
        end
    end
end

"""
    do_connect(twin::Twin)

Connect to the endpoint declared with `REMBUS_BASE_URL` env variable.

`REMBUS_BASE_URL` default to `ws://127.0.0.1:8000`

A component is considered anonymous when a different and random UUID is used as
component identifier each time the application connect to the broker.
"""
function do_connect(twin::Twin)
    if !isopen(twin.socket)
        router = last_downstream(twin.router)
        if router.settings.connection_mode === authenticated
            transport_connect(twin)
            await_challenge(router, twin)
        else
            transport_connect(twin)
            authenticate(router, twin)
        end
    end

    if isopen(twin.socket)
        if !isnothing(twin.connected)
            c = twin.connected
            # time granted to remote component for booting.
            Timer(0.1) do tmr
                put!(c, true)
            end
        end
        return true
    else
        return false
    end
end

function update_tables(router::Router, twin::Twin, exports)
    if isnothing(exports)
        return nothing
    end

    if ismultipath(router)
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
    end

    return nothing
end

#=
The connecting node declare its identity to the broker and authenticate if prompted.
=#
function authenticate(router::Router, twin::Twin)
    if !hasname(twin) || !requireauthentication(twin.socket)
        return nothing
    end

    meta = Dict(string(proto) => lner.port for (proto, lner) in router.listeners)
    msg = IdentityMsg(twin, cid(twin), meta)
    response = twin_request(twin, msg, router.settings.request_timeout)
    @debug "[$twin] authenticate: $response"
    if (response.status == STS_GENERIC_ERROR)
        close(twin.socket)
        throw(RembusError(code=STS_GENERIC_ERROR, reason=response_data(response)))
    elseif (response.status == STS_CHALLENGE)
        msg = attestate(router, twin, response)
        response = twin_request(twin, msg, router.settings.request_timeout)
    end

    if (response.status != STS_SUCCESS)
        # If an error occurs when authenticating than shutdown the component
        # to avoid reconnecting attempts.
        twin.process.phase = :closing
        close(twin.socket)
        rembuserror(code=response.status, reason=response_data(response))
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
    delete!(router.id_twin, rid(twin))
    twin.uid = RbURL(msg.cid)
    router.id_twin[rid(twin)] = twin
    setname(twin.process, rid(twin))
    twin.isauth = isauth
    load_twin(router, twin, router.store)
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
            rid(twin) !== rid(target)
        end
        if !isempty(vals)
            push!(results, topic)
        end
    end

    # Convert to a list for cbor encoding optimization.
    return collect(results)
end

function topic_impls(router::Router, target::Twin)
    results = filter(keys(router.local_function)) do topic
        # filter out the built-in methods from the list of exposers
        !haskey(router.local_subscriber, topic) && !isbuiltin(topic)
    end

    return _topics(results, target, router.topic_impls)
end

function topic_interests(router::Router, target::Twin)
    results = filter(keys(router.local_function)) do topic
        haskey(router.local_subscriber, topic)
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

        # The named component is connected,
        # send a message to component_info topic.
        twin_event(twin, "connection_up")

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
                    rid(twin),
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
        if !isa(twin.socket, Float)
            @error "[$twin] receiver error: $e"
        end
    end
end

function remove_twin(router::Router, twin::Twin)
    delete!(router.id_twin, rid(twin))
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
    remove_twin(router, twin)
    return nothing
end

function end_receiver(twin::Twin)
    if hasname(twin)
        detach(twin)
    else
        destroy_twin(twin, last_downstream(twin.router))
    end
end

sendto_origin(::Twin, ::FutureResponse) = false # COV_EXCL_LINE

function sendto_origin(twin::Twin, ::WsPing)
    if (isa(twin.socket, WS) && isopen(twin.socket.sock.io))
        WebSockets.ping(twin.socket.sock)
    end

    return true
end

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
    isauthenticated(rb)

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
        qos = msg.flags & QOS2
        if qos > QOS0
            put!(twin.process.inbox, AckMsg(twin, msg.id))
        end
        if qos == QOS2
            if already_received(twin, msg)
                @info "[$twin] skipping already received message $msg"
                return true
            else
                add_pubsub_id(twin, msg)
            end
        end
        if router.settings.archiver_interval > 0
            push!(router.archiver.inbox, msg)
        end
        # Publish to interested twins.
        local_subscribers(router, msg.twin, msg)
        msg.counter = uts()
        # Relay to subscribers of this broker.
        broadcast_msg(router, msg)
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

        if (sock.out[msgid].request.flags & QOS2) == QOS2
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
    @debug "admin_msg: $msg"
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
        @debug "admin_msg res: $admin_res"
        put!(twin.process.inbox, admin_res)
    end
    return true
end

function manage_target(router, twin, target_twin, msg)
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
            manage_target(router, twin, target_twin, msg)
        elseif any(id -> startswith(id, msg.target * "@"), keys(router.id_twin))
            targets = filter(p -> startswith(p.first, msg.target * "@"), router.id_twin)
            target_twin = select_twin(router, domain(twin), msg.topic, values(targets))
            manage_target(router, twin, target_twin, msg)
        else
            # target twin is unavailable
            m = ResMsg(msg, STS_TARGET_NOT_FOUND, msg.target)
            put!(twin.process.inbox, m)
        end
    else
        # msg is routable, try to resolve locally or get it to selected twin
        @debug "[$twin] to router: $msg"
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

function send_data_at_rest(twin::Twin, from_msg::Float64, ::FileStore)
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

function start_reactive(pd, twin::Twin, from_msg::Float64)
    twin.reactive = true
    @debug "[$twin] start reactive from: $(from_msg)"
    router = last_downstream(twin.router)
    return send_data_at_rest(twin, from_msg, router.store)
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
    future = Channel{UInt8}(1)
    t = Timer(router.settings.request_timeout) do _t
        put!(future, STS_GENERIC_ERROR)
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

function create_request(twin, msg_id::Msgid, request::AbstractString, params)
    if contains(request, '/')
        target = request[1:(findlast(==('/'), request)-1)]
        topic = request[(findlast(==('/'), request)+1):end]
    else
        target = nothing
        topic = request
    end

    return RpcReqMsg(twin, msg_id, topic, params, target)
end

"""
    jsonrpc_request(pkt::Dict, msg_id, params) -> RembusMsg

Parse a JSON-RPC request and return the appropriate RembusMsg subtype.
"""
function jsonrpc_request(twin, pkt::Dict, msg_id, params)
    if isa(params, Vector) || isnothing(params)
        # Default to RPC request
        return create_request(twin, msg_id, pkt["method"], params)
    elseif isa(params, Dict)
        msg_type = get(params, "__type__", nothing)
        if msg_type in (QOS1, QOS2)
            return PubSubMsg(
                twin,
                pkt["method"],
                get(params, "data", nothing),
                msg_type,
                msg_id,
            )
        elseif msg_type == TYPE_IDENTITY
            return IdentityMsg(
                twin,
                msg_id,
                params["cid"],
                get(params, "meta", nothing)
            )
        elseif msg_type == TYPE_ADMIN
            return AdminReqMsg(
                twin,
                msg_id,
                pkt["method"],
                get(params, "data", nothing)
            )
        elseif msg_type == TYPE_ATTESTATION
            sig = params["signature"]
            return Attestation(
                twin,
                msg_id,
                pkt["method"],
                base64decode(sig),
                get(params, "meta", nothing)
            )
        elseif msg_type == TYPE_REGISTER
            pubkey = get(params, "key_val", nothing)
            if isa(pubkey, String)
                pubkey = base64decode(pubkey)
            end
            return Register(
                twin,
                msg_id,
                pkt["method"],
                params["pin"],
                pubkey,
                UInt8(get(params, "key_type", SIG_RSA))
            )
        elseif msg_type == TYPE_UNREGISTER
            return Unregister(twin, msg_id)
        else
            return create_request(twin, msg_id, pkt["method"], params)
        end
    else
        error("$(pkt): invalid JSON-RPC request")
    end
end

function jsonprc_response(twin, pkt, msg_id, result)::RembusMsg
    """Parse a JSON_RPC success response"""

    msg_type = get(result, "__type__", missing)
    if ismissing(msg_type) || msg_type == TYPE_RESPONSE
        status = get(result, "sts", STS_SUCCESS)
        return ResMsg(twin, msg_id, status, get(result, "data", nothing))
    elseif msg_type == TYPE_ACK
        return AckMsg(twin, msg_id)
    elseif msg_type == TYPE_ACK2
        return Ack2Msg(twin, msg_id)
    end

    error("$pkt:invalid JSON-RPC response")
end

function json_parse(twin::Twin, pkt::Dict)
    @debug "[$twin] json_parse: $pkt"
    uid = get(pkt, "id", missing)
    if ismissing(uid)
        # Assume a PubSub message.
        return PubSubMsg(twin, pkt["method"], get(pkt, "params", nothing))
    else
        if isa(uid, String)
            msg_id = parse(Msgid, uid)
        else
            msg_id = Msgid(uid)
        end

        result = get(pkt, "result", nothing)
        if !isnothing(result)
            return jsonprc_response(twin, pkt, msg_id, result)
        end

        err = get(pkt, "error", nothing)
        if !isnothing(err)
            return jsonprc_response(twin, pkt, msg_id, err)
        end

        # Request-Response message.
        params = get(pkt, "params", nothing)
        return jsonrpc_request(twin, pkt, msg_id, params)
    end
end

function twin_event(twin, event::AbstractString; detail=missing)
    data = Dict("event" => event, "cid" => rid(twin))

    #if !ismissing(detail)
    #    data["detail"] = detail
    #end
    @debug "[$twin] twin_event: $data"
    msg = PubSubMsg(
        twin,
        "component_info",
        [data]
    )
    put!(twin.router.process.inbox, msg)
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

            if isa(payload, String)
                twin.enc = JSON
                pkt = JSON3.read(payload, Dict)
                msg::RembusMsg = json_parse(twin, pkt)
            else
                twin.enc = CBOR
                msg = broker_parse(twin, payload)
            end

            @debug "[$(path(twin))] twin_receiver << $msg"
            put!(twin.router.process.inbox, msg)
        end
    catch e
        receiver_exception(twin, e)
        dumperror(twin, e)
    finally
        end_receiver(twin)

        # Send the connection_down message to component_info topic.
        twin_event(twin, "connection_down")

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
    @debug "$(proc.supervisor) listening at port $proto:$port"

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
    # Remove the connected message bookmarker
    if !isnothing(twin.connected)
        take!(twin.connected)
    end
    # save the state to disk
    router = last_downstream(twin.router)
    if !isrepl(twin.uid)
        save_twin(router, twin, router.store)
    end

    if !isa(twin.socket, Float)
        # Move the outstanding requests to the floating socket
        # This will trigger the requests timeout ...
        twin.socket = Float(twin.socket.out, twin.socket.direct)

        if !isempty(twin.ackdf)
            save_received_acks(twin, router.store)
        end

        # Remove the twin from the router tables.
        if !hasname(twin)
            cleanup(twin, router)
        end
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
        @debug "starting twin [$(rid(twin))]"
        for msg in self.inbox
            max_retries = last_downstream(twin.router).settings.send_retries
            if isshutdown(msg)
                self.phase = :closing
                break
            else
                if !sendto_origin(twin, msg)
                    done = false
                    retries = 0
                    while !done && retries <= max_retries && isopen(twin)
                        done = message_send(twin, msg)
                        retries += 1
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
