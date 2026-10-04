settings(twin::Twin) = top_router(twin.router).settings

#=
Start the twin process and add to the router id_twin map.
=#
function start_twin(router::Router, twin::Twin)
    id = rid(twin)
    spec = process(id, twin_task, args=(twin,), force_interrupt_after=30.0)
    twin.process = spec
    twin_sv = Visor.from_supervisor(router.process.supervisor, "twins")
    startup(twin_sv, spec)

    yield()
    router.id_twin[id] = twin
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
            router = top_router(twin.router)
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

"""
    await_challenge(router::Router, twin::Twin)

Client side counterpart of [`do_connect`](@ref) for brokers configured with
`connection_mode === authenticated`: instead of proactively sending an
`IdentityMsg` (see [`authenticate`](@ref)), wait for the peer to send a
challenge first (registered as a pending future keyed by `CONNECTION_ID`)
and block until it is answered, raising on timeout or authentication
failure. Table synchronization still happens the same way once the
attestation completes, via [`update_tables`](@ref).
"""
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
    router = top_router(twin.router)
    router.settings.ws_ping_interval == 0 && return
    while true
        sleep(router.settings.ws_ping_interval)
        put!(twin.process.inbox, WsPing())
    end
end

acks_file(r::Router, id::AbstractString) = joinpath(rembus_dir(), r.id, "$id.acks")


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
            elseif isa(e, ZMQ.TimeoutError)
                # Just a periodic poll timeout (see ZDealer's rcvtimeo):
                # give `close` a chance to observe that this task is idle
                # and stop looping once the socket is being closed.
                twin.socket.closing[] && break
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
    rb.socket.task[] = @async zmq_receive(rb)
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
            cacert = ENV["HTTP_CA_BUNDLE"]
            @debug "cacert: $cacert"
            tls_config = HTTP.TLS.Config(ca_file=cacert)
            client = HTTP.Client(transport=HTTP.Transport(tls_config=tls_config))
            HTTP.WebSockets.open(socket -> begin
                    rb.socket = WS(socket)
                    notify(isconnected)
                    @async keep_alive(rb)
                    twin_receiver(rb)
                end, url; client=client)
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
                end, url)
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

function gethost(remote_ip)
    # Reverse DNS lookup
    rhost = try
        getnameinfo(remote_ip)
    catch
        string(remote_ip)
    end
    return rhost
end

function socketaddr_ip(addr::HTTP.TCP.SocketAddrV4)
    return Sockets.IPv4(addr.ip...)
end

function socketaddr_ip(addr::HTTP.TCP.SocketAddrV6)
    b = addr.ip
    groups = ntuple(i -> (UInt16(b[2i-1]) << 8) | UInt16(b[2i]), 8)
    return Sockets.IPv6(groups...)
end

function remote_ip(socket::WS)
    conn = socket.sock.stream
    addr = isa(conn, HTTP.TLS.Conn) ? HTTP.TLS.remote_addr(conn) : HTTP.TCP.remote_addr(conn)
    return addr === nothing ? nothing : socketaddr_ip(addr)
end

remote_ip(socket::TCP) = getpeername(socket.sock)[1]

remote_ip(socket::TLS) = getpeername(socket.sock.bio)[1]

remote_ip(socket::ZDealer) = nothing

function remote_host(socket)
    ip = remote_ip(socket)
    return ip === nothing ? "unknown" : gethost(ip)
end


"""
    component(urls::Vector)

Start a component that connects to a pool of nodes defined by the `urls` array.
"""
function component(
    urls::Vector;
    datadir=nothing,
    dbpath=nothing,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR,
    keyspace=false
)
    if ismissing(name)
        nodeurl = localcid()
        name = RbURL(nodeurl).id
    end

    router = get_router(
        name=name,
        datadir=datadir,
        dbpath=dbpath,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        authenticated=authenticated,
        secure=secure,
        keyspace=keyspace
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
    url::RbURL;
    datadir=nothing,
    dbpath=nothing,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    name=missing,
    secure=false,
    authenticated=false,
    policy="first_up",
    enc=CBOR,
    keyspace=false,
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
        name=name,
        datadir=datadir,
        dbpath=dbpath,
        ws=ws,
        tcp=tcp,
        zmq=zmq,
        http=http,
        authenticated=authenticated,
        secure=secure,
        keyspace=keyspace
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

    ## Load installed services and subscribers
    #load_callbacks(twin)

    return handle_connection(twin, failovers)
end

"""
    handle_connection(twin::Twin, failovers)

Setup the twin connection handling and initiate the connection.

* Add failover URLs to the twin and setup the reconnect handler.
* Setup the reconnect handler.
* Connect the twin.
"""
function handle_connection(twin::Twin, failovers)
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
        @error "[$twin]: $(isa(e, HTTP.ConnectError) ? e.cause : e)"
        down_handler(twin)
    finally
    end

    return twin
end

function connect(
    url::RbURL;
    name=missing,
    datadir=nothing,
    dbpath=nothing,
    enc=CBOR)
    if ismissing(name)
        name = url.id
    end

    router = get_router(name=name, datadir=datadir, dbpath=dbpath, keyspace=false)
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
    router = top_router(twin.router)
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

"""
    reconnect(twin::Twin, url::RbURL)

Attempt to reconnect to the given `url`.

The `url` is the primary url of the twin or one of its failover urls.`

On a successful [`do_connect`](@ref), re-synchronizes mesh state with
`twin_setup` (sends the explicit `SETUP_CMD` admin command with
this node's current exports, see `admin_command`): since a reconnection
may land on a broker that lost all prior incremental
`subscribe`/`expose` state for this link, the full local configuration is
replayed in one shot rather than relying on the one-time attestation
exchange used for a fresh connection (see [`authenticate`](@ref)). If this
re-sync fails, the connection is torn down again rather than left in an
inconsistent state.

If this twin exposes listener ports (it is itself a broker) and the new
connection succeeded, any other currently-connected node is disconnected
too, forcing them to reconnect to the (possibly new) primary broker rather
than staying attached to a stale failover.
"""
function reconnect(twin::Twin, url::RbURL)
    twin.uid = url
    isconnected = false
    try
        if do_connect(twin)
            router = top_router(twin.router)

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
                send_data_at_rest(twin, twin.failover_from, router.con)
            end
        end
    catch e
        @debug "[$twin] reconnecting error: ($e)"
    end

    return isconnected
end

"""
    reconnect(twin::Twin)

Reconnection loop driver: repeatedly try `twin.failovers` in order
(sleeping `reconnect_period` between attempts) via
[`reconnect`](@ref)`(twin, url)` until one succeeds or the twin is closing.
Installed as the `HR_CONN_DOWN` handler in [`handle_connection`](@ref), it
is what makes a mesh link self-heal (and re-run mesh table
synchronization) after a transient network failure.
"""
function reconnect(twin::Twin)
    twin.process.phase === :closing && return
    @debug "[$twin] reconnecting..."
    period = top_router(twin.router).settings.reconnect_period
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

`REMBUS_BASE_URL` default to `ws://127.0.0.1:8338`

A component is considered anonymous when a different and random UUID is used as
component identifier each time the application connect to the broker.

Performs the transport-level connection and, for non pre-shared-key
authentication, the identity/attestation handshake (see
[`authenticate`](@ref) and [`await_challenge`](@ref)) that seeds this
node's mesh routing tables with the peer's exports via
[`update_tables`](@ref).
"""
function do_connect(twin::Twin)
    if !isopen(twin.socket)
        router = top_router(twin.router)
        if router.settings.connection_mode === authenticated
            transport_connect(twin)
            await_challenge(router, twin)
        else
            transport_connect(twin)
            authenticate(router, twin)
        end
    end

    if isopen(twin.socket)
        load_callbacks(twin)
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

"""
    update_tables(router::Router, twin::Twin, exports)

Register the peer's already-known topics into the local mesh routing
tables when a connection is (re)established.

`exports` is the two-element list `[exposers, subscribers]` returned by
the peer's `get_topics` (via the attestation response, see
[`authenticate`](@ref)): `exports[1]` are topics the peer implements,
`exports[2]` are topics the peer (or some twin reachable through it) is
subscribed to. For each, `twin` is added to `router.topic_impls`/
`router.topic_interests`, so this router can immediately forward RPC
requests and Pub/Sub messages towards the peer without waiting for
individual `expose`/`subscribe` admin commands to propagate.

A no-op if `exports` is `nothing` or [`ismultipath`](@ref) is `false`.
"""
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
            mark_glob_topic!(router, topic)
        end
    end

    return nothing
end

#=
The connecting node declare its identity to the broker and authenticate if prompted.
=#
"""
    authenticate(router::Router, twin::Twin)

Client side of the connection handshake: declare this node's identity to
the peer by sending an `IdentityMsg` (carrying `cid(twin)` and the local
listener ports) and, if challenged, reply with the signed attestation (see
`attestate`).

On success, the peer's response carries its view of the mesh (its own
local methods/subscriptions plus whatever it already knows through other
connected twins, computed by `get_topics` on the peer side) and is applied
locally with [`update_tables`](@ref), seeding `router.topic_impls`/
`router.topic_interests` for the twin representing this link. This is the
mirror operation of [`attestation`](@ref), which runs on the peer that
*accepts* the connection.

No-op if `twin` is anonymous or the transport does not require
authentication.
"""
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

"""
    setidentity(router, twin, msg; isauth=false)

Update twin identity parameters.

Re-keys `router.id_twin` from the (likely ephemeral) previous id to
`rid(twin)` after `twin.uid` is set from `msg.cid`, so the mesh's
neighbor-lookup table (`id_twin`, used by [`admin_broadcast`](@ref) and
direct-RPC target resolution) reflects the component's definitive name as
soon as it authenticates.
"""
function setidentity(router::Router, twin::Twin, msg; isauth=false)
    delete!(router.id_twin, rid(twin))
    twin.uid = RbURL(msg.cid)
    router.id_twin[rid(twin)] = twin
    setname(twin.process, rid(twin))
    twin.isauth = isauth
    load_twin(router, twin, router.con)
    setup_twin(twin.router, twin)
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
        @info "verify signature: $e"
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


"""
    _topics(results, target::Twin, topic_map)

Shared helper for [`topic_impls`](@ref) and [`topic_interests`](@ref):
extend the `results` collection with every key of `topic_map` (either
`router.topic_impls` or `router.topic_interests`) that has at least one
twin other than `target` associated with it, then return `results` as a
`Vector` (CBOR encodes vectors more compactly than sets).

Excluding `target` ensures a peer is never told about its own
topic — relevant when the attestation response is computed for the very
twin whose link is being (re)established.
"""
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

"""
    topic_impls(router::Router, target::Twin)

Compute the list of topics `target` should be told this router implements
(or knows an implementor for): the router's own local Julia methods
(`router.local_function`, excluding subscribers and built-ins) unioned,
via [`_topics`](@ref), with every topic in `router.topic_impls` that has
an implementor other than `target` itself.

Used to build the exports sent back in the attestation response — see
[`get_topics`](@ref) and [`attestation`](@ref).
"""
function topic_impls(router::Router, target::Twin)
    results = filter(keys(router.local_function)) do topic
        # filter out the built-in methods from the list of exposers
        !haskey(router.local_subscriber, topic) && !isbuiltin(topic)
    end

    return _topics(results, target, router.topic_impls)
end

"""
    topic_interests(router::Router, target::Twin)

Compute the list of topics `target` should be told this router (or some
twin reachable through it) is subscribed to: the router's own local
subscriber callbacks (`router.local_function` filtered by
`router.local_subscriber`) unioned, via [`_topics`](@ref), with every
topic in `router.topic_interests` that has a subscriber other than
`target` itself.

Used to build the exports sent back in the attestation response — see
[`get_topics`](@ref) and [`attestation`](@ref).
"""
function topic_interests(router::Router, target::Twin)
    results = filter(keys(router.local_function)) do topic
        haskey(router.local_subscriber, topic)
    end

    return _topics(results, target, router.topic_interests)
end

"""
    get_topics(r::Router, target::Twin)

Return `[`[`topic_impls`](@ref)`(r, target), `[`topic_interests`](@ref)`(r, target)]`,
the pair of exports lists sent to `target` as the `reason` payload of a
successful attestation response (see [`attestation`](@ref)). The connecting
peer applies them with [`update_tables`](@ref), completing the handshake
half of mesh table synchronization (the other half being the `SETUP_CMD`
admin command, see `admin_command`).
"""
function get_topics(r::Router, target::Twin)
    return [topic_impls(r, target), topic_interests(r, target)]
end

"""
    attestation(router, twin, msg, authenticate=true)

Server side of the connection handshake, run when an `IdentityMsg` is
received from a connecting component: authenticate the client (unless
`authenticate` is `false`), bind its identity with `setidentity`, and
reply with `get_topics(router, twin)` as the response payload — this
router's own exports (local methods/subscribers plus whatever is already
reachable through other connected twins). The peer applies this payload
with [`update_tables`](@ref) on its side (see [`authenticate`](@ref)),
which is how a newly joined mesh node immediately learns the routing state
of the broker it just connected to.

If authentication fails, the websocket is closed instead.
"""
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
        twin_up(twin)

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
                    string(remote_ip(twin.socket)), msg.meta
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
        if close_is_ok(twin.socket, e) || twin.process.phase === :closing
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

"""
    remove_twin(router::Router, twin::Twin)

Remove `twin` from `router.id_twin`, dropping it from the set of direct
mesh neighbors reachable from `router` (it will no longer be a candidate
for [`admin_broadcast`](@ref), direct-RPC target resolution, or any other
neighbor lookup). Does not touch `topic_impls`/`topic_interests`; that
cleanup is `cleanup`'s responsibility.
"""
function remove_twin(router::Router, twin::Twin)
    delete!(router.id_twin, rid(twin))
end

"""
    destroy_twin(twin, router)

Remove the twin from the system.

Detach `twin`, shut down its `Visor` process, and remove it from the
router with [`remove_twin`](@ref), fully evicting it as a mesh neighbor.
Called for anonymous (unnamed) twins when their receiver loop ends; named
twins are kept around (just detached) to allow reconnection, see
[`end_receiver`](@ref).
"""
function destroy_twin(twin::Twin, router::Router)
    detach(twin)
    if isdefined(twin, :process)
        Visor.shutdown(twin.process)
    end
    remove_twin(router, twin)
    return nothing
end

"""
    end_receiver(twin::Twin)

Called when a twin's receiver loop terminates (connection closed): keep
named twins around (just `detach`ed, so reconnection can reuse the same
mesh-routing entries once the link comes back up) but fully evict
anonymous twins with [`destroy_twin`](@ref), since an anonymous component
has no stable identity to reconnect against.
"""
function end_receiver(twin::Twin)
    if hasname(twin)
        detach(twin)
    else
        destroy_twin(twin, top_router(twin.router))
    end
end

sendto_origin(::Twin, ::FutureResponse) = false # COV_EXCL_LINE

function sendto_origin(twin::Twin, ::WsPing)
    if (isa(twin.socket, WS) && isopen(twin.socket.sock))
        WebSockets.ping(twin.socket.sock)
    end

    return true
end

"""
    sendto_origin(twin, msg)

Short-circuit a response that targets a locally-pending direct request,
without going through the full `broadcast_msg`/mesh-relay path.

If `twin.socket.direct` has a future registered under `msg.id` (i.e. this
twin itself issued the matching direct request — such as an admin command
or a direct RPC call awaiting synchronous completion), fulfill that future
with `msg`, cancel its timeout timer and return `true`. Returns `false`
when there is no such pending future, meaning the caller (`respond`,
`admin_broadcast`) must fall back to delivering `msg` through the normal
inbox/broadcast mechanism so it keeps propagating towards its real
destination across the mesh.

Method-specific overloads exist for `FutureResponse` (always `false`,
nothing to resolve) and `WsPing` (sends a WebSocket ping frame instead of
resolving a future).
"""
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

"""
    command_permitted(router::Router, twin::Twin)

Return `true` unless `router` requires authentication
(`router_isauthenticated`) and `twin` has not authenticated
(`isauthenticated`), in which case the socket is closed and `false` is
returned. Guards every inbound Pub/Sub ([`pubsub_msg`](@ref)), RPC
([`rpc_request`](@ref)) and admin ([`admin_msg`](@ref)) message before any
mesh-routing logic (authorization, broadcast, forwarding) is applied.
"""
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

"""
    pubsub_msg(router::Router, msg)

Handle an inbound `PubSubMsg`: after the [`command_permitted`](@ref)/
[`isauthorized`](@ref) checks, (QoS2 de-duplication aside) deliver it in
two complementary ways so it reaches every interested party regardless of
how far away they are in the mesh:

- `local_subscribers(router, msg.twin, msg)`: run any Julia callback bound
  directly on this router for `msg.topic` (exact or glob match);
- `broadcast_msg(router, msg)`: `put!` the message onto the inbox of every
  twin in `router.topic_interests[msg.topic]` (plus glob subscribers),
  which includes twins that are themselves neighbor brokers — causing
  `pubsub_msg` to run again on the next hop and the message to keep
  walking the mesh towards the real subscribers. See the
  [Mesh Routing and Forwarding](@ref) guide.

Returns `false` (without delivering) if the permission/authorization
checks fail, `true` otherwise.
"""
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

"""
    admin_msg(router::Router, msg)

Entry point for inbound `AdminReqMsg`s received on a twin's connection:
after the [`command_permitted`](@ref) check, dispatch to
[`admin_command`](@ref) and deliver the result back onto `twin`'s own
inbox:

- if the command returned `EnableReactiveMsg` (the `REACTIVE_CMD` case),
  start a dedicated `start_reactive` process to replay data-at-rest and
  reply `STS_SUCCESS` directly;
- otherwise the `ResMsg` produced by [`admin_command`](@ref) is delivered
  as-is (it may need to travel back through `respond`/`sendto_origin` if
  the requestor is several hops away).

Returns `false` without doing anything else if the permission check
fails.
"""
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

"""
    manage_target(router, twin, target_twin, msg)

Deliver a *directed* RPC request/response `msg` (one that named an
explicit `msg.target` component or group, see [`rpc_request`](@ref)) to
`target_twin`:

- `STS_TARGET_DOWN` back to `twin` if `target_twin`'s socket is closed;
- resolve it locally via `local_fn` if `target_twin === twin` (loopback
  call to a method exposed by the requestor itself);
- otherwise forward `msg` onto `target_twin`'s inbox, provided the topic is
  actually known (`router.topic_impls` or a built-in command) — letting
  `target_twin` (possibly itself a neighbor broker) continue resolving or
  relaying it further along the mesh; `STS_METHOD_NOT_FOUND` is replied
  otherwise.
"""
function manage_target(router, twin, target_twin, msg)
    if !isopen(target_twin)
        m = ResMsg(msg, STS_TARGET_DOWN, msg.target)
        put!(twin.process.inbox, m)
    else
        if target_twin == twin
            local_fn(router, twin, msg)
        else
            # check if remote expose the topic
            if haskey(router.topic_impls, msg.topic) ||
               msg.topic in BUILTINS_CMD
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

"""
    rpc_request(router::Router, msg, implementor_rule)

Handle an inbound `RpcReqMsg`, after the [`command_permitted`](@ref)/
[`isauthorized`](@ref) checks:

- **Direct RPC** (`msg.target !== nothing`, i.e. the caller named a
  specific component or group): resolve `msg.target` against
  `router.id_twin` — either an exact id match, or a `"<group>@..."`
  pooled-member match (in which case `select_twin` applies the
  router's load-balancing policy among the matching members) — and
  delegate delivery to [`manage_target`](@ref). `STS_TARGET_NOT_FOUND` is
  replied if no matching twin is connected here.
- **Routable RPC** (`msg.target === nothing`): try the router's own
  `local_fn` first; otherwise, if `implementor_rule(twin.uid)` allows it
  (used to prevent re-forwarding loops/policy restrictions upstream),
  call `find_implementor(router, msg)` to pick a connected exposer —
  possibly itself a neighbor broker, in which case this same function
  runs again there, hopping the request towards the real implementor
  across the mesh.

Returns `msg` unchanged (the various outcomes are delivered as side
effects by `put!`ing responses/forwarded requests onto the relevant
twins' inboxes).
"""
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


function start_reactive(pd, twin::Twin, msg_from::Float64)
    twin.reactive = true
    @debug "[$twin] start reactive from: $(msg_from)"
    router = top_router(twin.router)
    return send_data_at_rest(twin, msg_from, router.con)
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
        twin_down(twin)
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
    ra = repr(UInt64(pointer_from_objref(router)))
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
    proto = (sslconfig === nothing) ? "ws" : "wss"
    @debug "$(proc.supervisor) listening at port $proto:$port"

    setphase(proc, :listen)

    server = HTTP.WebSockets.listen!(
        IP,
        port,
        tls_config=sslconfig
    ) do ws
        client_receiver(router, WS(ws))
    end
    ws_server!(router, server)
    return server
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
    router = top_router(twin.router)
    if !isbroker(twin.uid) && hasname(twin)
        save_twin(router, twin, router.con)
    end

    if !isa(twin.socket, Float)
        # Move the outstanding requests to the floating socket
        # This will trigger the requests timeout ...
        twin.socket = Float(twin.socket.out, twin.socket.direct)

        if !isempty(twin.ackdf) && hasname(twin)
            save_received_acks(twin, router.con)
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
            max_retries = top_router(twin.router).settings.send_retries
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
                    if isa(msg, FutureResponse) && isa(msg.request, PubSubMsg) &&
                       !isready(msg.future)
                        # Retries exhausted (or twin was never open): unblock
                        # the publish()/put() caller waiting for the ack.
                        put!(msg.future, done)
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
