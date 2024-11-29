@enum QoS fast with_ack

struct EnableReactiveMsg <: RembusMsg
    id::UInt128
    msg_from::Float64
end

#=
Twin is the broker-side image of a component.

`socket` is the socket handle when the protocol is tcp/tls or ws/wss.

For ZMQ sockets one socket is shared between all twins.
=#
mutable struct Twin
    type::NodeType
    router::AbstractRouter
    id::String
    hasname::Bool
    isauth::Bool
    reactive::Bool
    session::Dict{String,Any}
    egress::Union{Nothing,Function}
    ingress::Union{Nothing,Function}
    socket::Any
    out::Dict{UInt128,Distributed.Future}
    acktimer::Dict{UInt128,AckState}
    zaddress::Vector{UInt8}
    mark::UInt64
    msg_from::Dict{String,Float64}
    sent::Dict{UInt128,Any} # msg.id => timestamp of sending
    process::Visor.Process
    zmqcontext::ZMQ.Context # for a Twin that establishes the connection to a component

    Twin(router, id, type::NodeType) = new(
        type,
        router,
        id,
        false,
        false,
        false,
        Dict(),
        nothing,
        nothing,
        nothing,
        Dict(),
        Dict(),
        UInt8[0, 0, 0, 0],
        0,
        Dict(),
        Dict()
    )
end

ucid(twin::Twin) = twin.id

hasname(twin::Twin) = twin.hasname

mutable struct Msg
    ptype::UInt8
    content::RembusMsg
    twchannel::Twin
    counter::UInt64
    reqdata::Any
    Msg(ptype, content, src) = new(ptype, content, src, 0)
    Msg(ptype, content, src, reqdata) = new(ptype, content, src, 0, reqdata)
end

struct SentData
    sending_ts::Float64
    request::Msg
    timer::Timer
end

msg_dataframe() = DataFrame(
    ptr=UInt[], ts=UInt[], uid=UInt[], topic=String[], pkt=String[]
)

mutable struct Router <: AbstractRouter
    id::String
    mode::ConnectionMode
    policy::Symbol
    msg_df::DataFrame
    mcounter::UInt64
    start_ts::Float64
    servers::Set{String}
    listeners::Dict{Symbol,Listener} # protocol => listener status
    address2twin::Dict{Vector{UInt8},Twin} # zeromq address => twin
    twin2address::Dict{String,Vector{UInt8}} # twin id => zeromq address
    topic_impls::Dict{String,OrderedSet{Twin}} # topic => twins implementor
    last_invoked::Dict{String,Int} # topic => twin index last called
    topic_interests::Dict{String,Set{Twin}} # topic => twins subscribed to topic
    id_twin::Dict{String,Twin}
    topic_function::Dict{String,Function}
    topic_auth::Dict{String,Dict{String,Bool}} # topic => {twin.id => true}
    admins::Set{String}
    twin_initialize::Function
    twin_finalize::Function
    pub_handler::Union{Nothing,Function}
    plugin::Union{Nothing,Module}
    shared::Any
    process::Visor.Process
    server::Sockets.TCPServer
    http_server::HTTP.Server
    ws_server::Sockets.TCPServer
    zmqsocket::ZMQ.Socket
    zmqcontext::ZMQ.Context
    owners::DataFrame
    component_owner::DataFrame
    Router(name::AbstractString, plugin=nothing, context=nothing) = new(
        name,
        anonymous,
        :first_up,
        msg_dataframe(),
        0,
        NaN,
        Set(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        Set(),
        twin_initialize,
        twin_finalize,
        nothing,
        plugin, # plugin
        context  # context
    )
end

Base.show(io::IO, r::Router) = print(io, r.id)

Base.hash(t::Twin, h::UInt) = hash(t.id, hash(:Twin, h))
Base.:(==)(a::Twin, b::Twin) = isequal(a.id, b.id)
Base.show(io::IO, t::Twin) = print(io, t.id)

function Base.show(io::IO, msg::Msg)
    if (isa(msg.content, ResMsg) || isa(msg.content, PubSubMsg)) &&
       isa(msg.content.data, Vector{UInt8})
        len = length(msg.content.data)
        cnt = len > 10 ? msg.content.data[1:10] : msg.content.data
        print(io, "binary[$len] $cnt ...")
    else
        print(io, "$(msg.content)")
    end
end

firstup_policy(router::Router) = router.policy = :first_up

roundrobin_policy(router::Router) = router.policy = :round_robin

lessbusy_policy(router::Router) = router.policy = :less_busy

forever(rb::Router) = !isinteractive() && wait(Visor.root_supervisor(rb.process))

#=
Return the subscribed pubsub topics of the twin
=#
function twin_topics(twin::Twin)
    topics = []
    for (k, v) in twin.router.topic_interests
        if twin in v
            push!(topics, k)
        end
    end
    return topics
end

#=
Methods related to the persistence of Pubsub messages.
=#

messages_fn(router, ts) = joinpath(messages_dir(router), string(ts))

#=
    Save pubsub message to in-memory cache and return the message pointer.

    For QOS1 or QOS2 levels the message id is used to match ACK and ACK2 messages.
=#
function save_message(router, msg::PubSubMsg)
    #ts::UInt64 = msg.id >> 64
    tv = Libc.TimeVal()
    ts = tv.sec * 1_000_000 + tv.usec
    uid::UInt64 = msg.id & 0xffffffffffffffff
    if isa(msg.data, IOBuffer)
        data = msg.data.data
    else
        data = encode_message(msg)
    end
    router.mcounter += 1

    push!(router.msg_df, [router.mcounter, ts, uid, msg.topic, String(data)])
    if (router.mcounter % 50000) == 0
        @debug "saved $(router.mcounter) messages"
    end

    if (router.mcounter % CONFIG.db_max_messages) == 0
        persist_messages(router)
        @debug "persisted $(router.mcounter) file"
        router.msg_df = msg_dataframe()
    end

    return router.mcounter
end

function data_at_rest(fn, broker="broker")
    path = joinpath(messages_dir(broker), fn)
    df = DataFrame(read_parquet(path))
    df.msg = decode.(Vector{UInt8}.(df.pkt))
    return df
end

function send_messages(twin::Twin, df)
    nowts = time() * 1_000_000
    for row in eachrow(df)
        msgid = UInt128(row.ts) << 64 + row.uid
        tmark = twin.mark
        if row.ptr > tmark
            if haskey(twin.msg_from, row.topic) && row.ts > (nowts - twin.msg_from[row.topic])
                pkt = Rembus.from_cbor(row.pkt)
                pkt.id = msgid
                msg = Msg(TYPE_PUB, pkt, twin)
                msg.counter = row.ptr
                put!(twin.process.inbox, msg)
            end
        end
    end
end

function from_disk_messages(twin::Twin, fn)
    path = joinpath(messages_dir(twin.router), fn)
    df = DataFrame(read_parquet(path))
    interests = twin_topics(twin)
    filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
    if !isempty(filtered)
        filtered.msg = decode.(Vector{UInt8}.(filtered.pkt))
        send_messages(twin, filtered)
    end
end

function from_memory_messages(twin::Twin)
    send_messages(twin, twin.router.msg_df)
end

file_lt(f1, f2) = parse(Int, f1) < parse(Int, f2)

function msg_files(router)
    return sort(readdir(Rembus.messages_dir(router)), lt=file_lt)
end

function persist_messages(router)
    fn = messages_fn(router, router.mcounter)
    @debug "[broker] persisting messages on disk: $fn"

    db = DuckDB.DB()
    DuckDB.register_data_frame(db, router.msg_df, "df")
    # do no overwrite file if no messages are published in the interval.
    if !isfile(fn)
        DBInterface.execute(
            db,
            "COPY df to '$(messages_fn(router, router.mcounter))' (FORMAT 'parquet')"
        )
    end

    close(db)
end

function start_reactive(twin::Twin, from_msg::Float64)
    twin.reactive = true
    @debug "[$twin] start reactive from: $(from_msg)"
    if twin.hasname && (from_msg > 0.0)
        # get the files with never sent messages
        allfiles = msg_files(twin.router)
        nowts = time()
        mdir = Rembus.messages_dir(twin.router)
        files = filter(allfiles) do fn
            if parse(Int, fn) <= twin.mark
                # the mesage was already delivered in a previous online session
                # of the component
                return false
            else
                ftime = mtime(joinpath(mdir, fn))
                delta = nowts - ftime
                if delta * 1_000_000 > from_msg
                    @info "skip $fn: mtime: $(unix2datetime(ftime)) ($delta secs from now)"
                    return false
                end
            end
            return true
        end
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
End of methods related to the persistence of Pubsub messages
=#

#=
macro mlog(str)
    quote
        if CONFIG.metering
            @info $(esc(str))
        end
    end
end
=#

macro rawlog(msg)
    quote
        if CONFIG.rawdump
            @info $(esc(msg))
        end
    end
end

Base.isopen(c::Condition) = true

offline(twin::Twin) = ((twin.socket === nothing) || !isopen(twin.socket))

offline(twin::RBServerConnection) = ((twin.socket === nothing) || !isopen(twin.socket))

session(twin::Twin) = twin.session

named_twin(id, router) = haskey(router.id_twin, id) ? router.id_twin[id] : nothing

router_supervisor(router::Router) = router.process.supervisor

router_supervisor(router::Server) = router.process

build_twin(router::Router, id, type) = Twin(router, id, type)

function build_twin(router::Server, id, type)
    rb = RBServerConnection(router, id, type)
    push!(router.connections, rb)
    if type === zrouter
        rb.socket = router.zmqsocket
    end
    return rb
end

twin_task(::Twin) = twin_task

twin_task(::RBServerConnection) = server_task

function create_twin(id, router::AbstractRouter, type::NodeType)
    if haskey(router.id_twin, id)
        tw = router.id_twin[id]
        tw.type = type
        return tw
    else
        twin = build_twin(router, id, type)
        spec = process(id, twin_task(twin), args=(twin,))
        startup(Visor.from_supervisor(router_supervisor(router), "twins"), spec)
        yield()
        router.id_twin[id] = twin
        router.twin_initialize(router.shared, twin)
        return twin
    end
end

#=
    offline!(twin)

Unbind the ZMQ socket from the twin.
=#
function offline!(twin)
    @debug "[$twin] closing: going offline"
    twin.socket = nothing
    delete!(twin.router.twin2address, ucid(twin))
    # Remove from address2twin
    filter!(((k, v),) -> twin != v, twin.router.address2twin)
    return nothing
end

#=
Remove the twin from the router tables.
=#
function cleanup(twin, router::Router)
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

    delete!(router.id_twin, twin.id)
    return nothing
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
    delete!(router.id_twin, twin.id)
    return nothing
end

function verify_signature(twin, msg)
    challenge = pop!(twin.session, "challenge")
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

#=
    setidentity(router, twin, msg; isauth=false, paging=true)

Update twin identity parameters.
=#
function setidentity(router, twin, msg; isauth=false, paging=true)
    # get the eventually created twin associate with cid
    namedtwin = create_twin(msg.cid, router, twin.type)
    # move the opened socket
    namedtwin.socket = twin.socket
    namedtwin.zaddress = twin.zaddress
    if !isa(twin.socket, ZMQ.Socket)
        twin.socket = nothing
        twin.zaddress = UInt8[0, 0, 0, 0]
    end

    # destroy the anonymous process
    destroy_twin(twin, router)
    namedtwin.hasname = true
    namedtwin.isauth = isauth
    return namedtwin
end

function setidentity(::Server, rb::RBServerConnection, msg; isauth=false, paging=true)
    rb.client.id = msg.cid
    rb.isauth = isauth
    return rb
end

function login(router, twin, msg)
    if haskey(router.topic_function, "login")
        router.topic_function["login"](twin, msg.cid, msg.signature) ||
            error("authentication failed")
    else
        verify_signature(twin, msg)
    end

    @debug "[$(msg.cid)] is authenticated"
    return nothing
end

#=
    attestation(router, twin, msg, authenticate=true, ispong=false)

Authenticate the client.

If authentication fails then close the websocket.
=#
function attestation(router, twin, msg, authenticate=true)
    @debug "[$twin] binding cid: $(msg.cid), authenticate: $authenticate"
    sts = STS_SUCCESS
    reason = nothing
    authtwin = nothing
    try
        if authenticate
            login(router, twin, msg)
        end
        authtwin = setidentity(router, twin, msg, isauth=authenticate)
        transport_send(Val(authtwin.type), authtwin, ResMsg(msg.id, sts, reason))
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
        transport_send(Val(twin.type), twin, ResMsg(msg.id, sts, reason))
    end

    if sts !== STS_SUCCESS
        detach(twin)
    end

    return authtwin
end

function attestation(router::Server, rb::RBServerConnection, msg)
    @debug "[$rb] authenticating cid: $(msg.cid)"
    sts = STS_SUCCESS
    reason = nothing
    try
        login(router, rb, msg)
        ### twin.id = msg.cid
        ### twin.hasname = true
        rb.isauth = true
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end

    response = ResMsg(msg.id, sts, reason)
    #@mlog("[$twin] -> $response")
    transport_send(Val(rb.type), rb, response)
    if sts !== STS_SUCCESS
        close(rb)
    end

    return sts
end

isenabled(router, tenant::Nothing) = true

function isenabled(router, tenant_id::AbstractString)
    df = router.owners[(router.owners.tenant.==tenant_id), :]
    if isempty(df)
        @info "unknown tenant [$tenant_id]"
        return false
    else
        if nrow(df) > 1
            @info "multiple tenants found for [$tenant_id]"
            return false
        end
        return (columnindex(df, :enabled) == 0) ||
               ismissing(df[1, :enabled]) ||
               (df[1, :enabled] === true)
    end
end

function get_token(router, tenant, id::UInt128)
    vals = UInt8[(id>>24)&0xff, (id>>16)&0xff, (id>>8)&0xff, id&0xff]
    token = bytes2hex(vals)
    df = router.owners[(router.owners.pin.==token).&(router.owners.tenant.==tenant), :]
    if isempty(df)
        @info "tenant [$tenant]: invalid token"
        return nothing
    else
        @debug "tenant [$tenant]: token is valid"
        return token
    end
end

#=
    register(router, msg)

Register a component.
=#
function register(router, msg)
    @debug "registering pubkey of $(msg.cid), id: $(msg.id), tenant: $(msg.tenant)"

    if !isenabled(router, msg.tenant)
        return ResMsg(msg.id, STS_GENERIC_ERROR, "tenant [$(msg.tenant)] not enabled")
    end

    sts = STS_SUCCESS
    reason = nothing
    if msg.tenant === nothing
        tenant = router.process.supervisor.id
    else
        tenant = msg.tenant
    end

    token = get_token(router, tenant, msg.id)
    if token === nothing
        sts = STS_GENERIC_ERROR
        reason = "wrong tenant/pin"
    elseif isregistered(router, msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        kdir = keys_dir(router)
        mkpath(kdir)

        save_pubkey(router, msg.cid, msg.pubkey, msg.type)
        if !(msg.cid in router.component_owner.component)
            push!(router.component_owner, [tenant, msg.cid])
        end
        save_token_app(router, router.component_owner)
    end

    return ResMsg(msg.id, sts, reason)
end

#=
    unregister(router, twin, msg)

Unregister a component.
=#
function unregister(router, twin, msg)
    @debug "[$twin] unregistering $(msg.cid), isauth: $(twin.isauth)"
    sts = STS_SUCCESS
    reason = nothing

    if !twin.isauth
        sts = STS_GENERIC_ERROR
        reason = "invalid operation"
    else
        remove_pubkey(router, msg.cid)
        deleteat!(router.component_owner, router.component_owner.component .== msg.cid)
        save_token_app(router, router.component_owner)
    end
    return ResMsg(msg.id, sts, reason)
end

function rpc_response(router, twin, msg)
    if haskey(twin.out, msg.id)
        put!(twin.out[msg.id], msg)
    end

    if haskey(twin.sent, msg.id)
        twinput = twin.sent[msg.id].request.twchannel
        reqdata = twin.sent[msg.id].request.content
        put!(router.process.inbox, Msg(TYPE_RESPONSE, msg, twinput, reqdata))

        elapsed = time() - twin.sent[msg.id].sending_ts
        #if CONFIG.metering
        #    @debug "[$(twin.id)][$(reqdata.topic)] exec elapsed time: $elapsed secs"
        #end
        close(twin.sent[msg.id].timer)
        delete!(twin.sent, msg.id)
    else
        @debug "[$twin] unexpected response: $msg"
    end
end

function admin_msg(router, twin, msg)
    admin_res = admin_command(router, twin, msg)
    @debug "sending admin response: $admin_res"
    if isa(admin_res, EnableReactiveMsg)
        put!(router.process.inbox, Msg(REACTIVE_MESSAGE, admin_res, twin))
    else
        put!(twin.process.inbox, admin_res)
    end
    return nothing
end

function rpc_request(router, twin, msg)
    if !isauthorized(router, twin, msg.topic)
        put!(
            twin.process.inbox,
            Msg(TYPE_RESPONSE, ResMsg(msg, STS_GENERIC_ERROR, "unauthorized"), twin)
        )
    elseif msg.target !== nothing
        # it is a direct rpc
        if haskey(router.id_twin, msg.target)
            target_twin = router.id_twin[msg.target]
            if offline(target_twin)
                put!(
                    twin.process.inbox,
                    Msg(TYPE_RESPONSE, ResMsg(msg, STS_TARGET_DOWN, msg.target), twin)
                )
            else
                put!(target_twin.process.inbox, Msg(TYPE_RPC, msg, twin))
            end
        else
            # target twin is unavailable
            put!(
                twin.process.inbox,
                Msg(TYPE_RESPONSE, ResMsg(msg, STS_TARGET_NOT_FOUND, msg.target), twin)
            )
        end
    else
        # msg is routable, get it to router
        @debug "[$twin] to router: $(prettystr(msg))"
        put!(router.process.inbox, Msg(TYPE_RPC, msg, twin, msg))
    end

    return nothing
end

#=
Get the message data payload.
Useful for content filtering by publish_interceptor().
=#
function msg_payload(io::IOBuffer)
    mark(io)
    payload = decode(io)
    reset(io)
    return payload
end

function get_router(broker="broker")::Router
    p = from("$broker.broker")
    if p === nothing
        error("unknown broker: $broker")
    end

    return p.args[1]
end

#=
Publish data on topic. Used by broker plugin module to republish messages
after transforming them.

The routing is performed by the broker
=#
function republish(twin, topic, data)
    new_msg = PubSubMsg(topic, data)
    put!(twin.router.process.inbox, Msg(TYPE_PUB, new_msg, twin))
end

function publish(router::Router, topic::AbstractString, data=[]; qos=QOS0)
    new_msg = PubSubMsg(topic, data, qos)
    put!(router.process.inbox, Msg(TYPE_PUB, new_msg, Twin(router, "tmp", loopback)))
end

#=
Publish data on topic. Used by broker plugin module to republish messages
after transforming them.

The message is delivered by the twin.
=#
function publish(twin::Twin, topic::AbstractString, data=[]; qos=QOS0)
    new_msg = PubSubMsg(topic, data, qos)
    put!(twin.process.inbox, Msg(TYPE_PUB, new_msg, twin))
end

function pubsub_msg(router, twin, msg)
    if !isauthorized(router, twin, msg.topic)
        @warn "[$twin] is not authorized to publish on $(msg.topic)"
    else
        if msg.flags > QOS0
            # reply with an ack message
            put!(twin.process.inbox, Msg(TYPE_ACK, AckMsg(msg.id), twin))
        end
        # msg is routable, get it to router
        pass = true
        if router.pub_handler !== nothing
            try
                pass = router.pub_handler(router.shared, twin, msg)
            catch e
                @error "publish_interceptor: $e"
            end
        end
        if pass
            @debug "[$twin] to router: $(prettystr(msg))"
            put!(router.process.inbox, Msg(TYPE_PUB, msg, twin))
        end
    end
    return nothing
end

function ack_msg(twin, msg)
    msgid = msg.id
    if haskey(twin.acktimer, msgid)
        close(twin.acktimer[msgid].timer)

        if twin.acktimer[msgid].ack2
            # send the ACK2 message to the component
            put!(
                twin.process.inbox,
                Msg(TYPE_ACK2, Ack2Msg(msgid), twin)
            )
        end
        put!(twin.out[msgid], true)
        delete!(twin.acktimer, msgid)
    end

    return nothing
end

function receiver_exception(router, twin, e)
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

function end_receiver(twin)
    twin.reactive = false
    if twin.hasname
        detach(twin)
    else
        destroy_twin(twin, twin.router)
    end
end

function common_receiver(router, twin, msg)
    if isa(msg, ResMsg)
        rpc_response(router, twin, msg)
    elseif isa(msg, AdminReqMsg)
        admin_msg(router, twin, msg)
    elseif isa(msg, RpcReqMsg)
        rpc_request(router, twin, msg)
    elseif isa(msg, PubSubMsg)
        pubsub_msg(router, twin, msg)
    elseif isa(msg, AckMsg)
        ack_msg(twin, msg)
    elseif isa(msg, Ack2Msg)
        # the broker ignores Ack2
    else
        error("unexpected rembus message")
    end
end

#=
    twin_receiver(router, twin)

Receive messages from the client socket (ws or tcp ).
=#
function twin_receiver(router, twin)
    @debug "client [$twin] is connected"
    try
        ws = twin.socket
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.socket = nothing
                @debug "component [$twin]: connection closed"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            #@mlog("[$(twin.id)] <- $(prettystr(msg))")

            if isa(msg, Unregister)
                response = unregister(router, twin, msg)
                put!(twin.process.inbox, response)
            else
                common_receiver(router, twin, msg)
            end
        end
    catch e
        receiver_exception(router, twin, e)
        @showerror e
    finally
        end_receiver(twin)
    end

    return nothing
end

function challenge(router, twin, msgid)
    if haskey(router.topic_function, "challenge")
        challenge = router.topic_function["challenge"](twin)
    else
        challenge = rand(RandomDevice(), UInt8, 4)
    end
    twin.session["challenge"] = challenge
    return ResMsg(msgid, STS_CHALLENGE, challenge)
end

#=
    anonymous_twin_receiver(router, twin)

Receive messages from the client socket (ws or tcp).
=#
function anonymous_twin_receiver(router, twin)
    @debug "anonymous client [$(twin.id)] is connected"
    try
        ws = twin.socket
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.socket = nothing
                @debug "component [$twin]: connection closed"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            #@mlog("[$(twin.id)] <- $(prettystr(msg))")
            if isa(msg, IdentityMsg)
                ret = identity_check(router, twin, msg, paging=true)
                if ret.sts === STS_SUCCESS
                    return ret.value
                end
            elseif isa(msg, Register)
                response = register(router, msg)
                put!(twin.process.inbox, response)
            elseif isa(msg, Attestation)
                return attestation(router, twin, msg)
            else
                common_receiver(router, twin, msg)
            end
        end
    catch e
        receiver_exception(router, twin, e)
        @showerror e
    finally
        end_receiver(twin)
    end

    return nothing
end


function close_connection(twin)
    @warn "[$twin] challenge not resolved: closing connection"
    end_receiver(twin)
end

#=
    authenticate_twin_receiver(router, twin)

Authenticate the remote node before trusting the connection.

Wait a finite amount of time for the challenge response.
=#
function authenticate_twin_receiver(router, twin)
    @debug "authenticating [$(twin.id)]"
    t = Timer((tmr) -> close_connection(twin), challenge_timeout())
    try
        ws = twin.socket
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                # tcp socket only: transport_read returns
                # an empty array when the connection is closed.
                twin.socket = nothing
                @debug "component [$twin]: connection closed"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            if isa(msg, IdentityMsg)
                # Manage the case when the component has not set mode to authenticated.
                ret = identity_check(router, twin, msg, paging=true)
                if ret.sts !== STS_CHALLENGE
                    error("[$(msg.cid)] authentication failed")
                end
            elseif isa(msg, Register)
                response = register(router, msg)
                put!(twin.process.inbox, response)
            elseif isa(msg, Attestation)
                return attestation(router, twin, msg)
            else
                error("not authenticated")
            end
        end
    catch e
        receiver_exception(router, twin, e)
        @showerror e
    finally
        close(t)
        end_receiver(twin)
    end

    return nothing
end


function commands_permitted(twin)
    if twin.router.mode === authenticated
        return isauthenticated(twin)
    end
    return true
end

#=
Parse the message and invoke the actions related to the message type.
=#
function eval_message(twin, msg, id=UInt8[])
    router = twin.router
    if isa(msg, IdentityMsg)
        @debug "[$twin] auth identity: $(msg.cid)"
        # check if cid is registered
        if key_file(router, msg.cid) !== nothing
            # authentication mode, send the challenge
            response = challenge(router, twin, msg.id)
            transport_send(Val(twin.type), twin, response)
        else
            identity_upgrade(router, twin, msg, id, authenticate=false)
        end
        #@mlog("[ZMQ][$twin] -> $response")
        callbacks(twin)
    elseif isa(msg, PingMsg)
        if (twin.id != msg.cid)

            # broker restarted
            # start the authentication flow if cid is registered
            @debug "lost connection to broker: restarting $(msg.cid)"
            if key_file(router, msg.cid) !== nothing
                # check if challenge was already sent
                if !haskey(twin.session, "challenge")
                    response = challenge(router, twin, msg.id)
                    transport_send(Val(twin.type), twin, response)
                end
            else
                identity_upgrade(router, twin, msg, id, authenticate=false)
            end

        else
            if twin.socket !== nothing
                pong(twin.socket, msg.id, id)
            end
        end
    elseif isa(msg, Attestation)
        identity_upgrade(router, twin, msg, id, authenticate=true)
    elseif isa(msg, Register)
        response = register(router, msg)
        put!(twin.process.inbox, response)
    elseif isa(msg, Close)
        offline!(twin)
    else
        command(router, twin, msg)
    end

    return nothing
end

#=
Twin receiver.

Parse the message received from a dedicated DEALER socket and inject into
the broker's router logic.
=#
function zmq_receive(rb::Twin)
    while true
        try
            msg = zmq_load(rb.socket)
            @async eval_message(rb, msg)
        catch e
            if !isopen(rb.socket)
                break
            else
                @error "zmq message decoding: $e"
                @showerror e
            end
        end
    end
    @debug "zmq socket closed"
end

function command(router::Router, twin, msg)
    if commands_permitted(twin)
        if isa(msg, Unregister)
            response = unregister(router, twin, msg)
            put!(twin.process.inbox, response)
        elseif isa(msg, ResMsg)
            rpc_response(router, twin, msg)
        elseif isa(msg, AdminReqMsg)
            admin_msg(router, twin, msg)
        elseif isa(msg, RpcReqMsg)
            rpc_request(router, twin, msg)
        elseif isa(msg, PubSubMsg)
            pubsub_msg(router, twin, msg)
        elseif isa(msg, AckMsg)
            ack_msg(twin, msg)
        elseif isa(msg, Ack2Msg)
            # the broker ignores Ack2 message
        end
    else
        @info "[$twin]: [$msg] not authorized"
        offline!(twin)
    end
end


function command(router::Server, twin, msg)
    if isa(msg, AdminReqMsg)
        admin_msg(router, twin, msg)
    else
        @async handle_input(twin, msg)
    end
end

#=
Broker and Server zmq receiver.
=#
function zeromq_receiver(router)
    pkt = ZMQPacket()
    while true
        try
            zmq_message(router, pkt)
            id = pkt.identity

            if haskey(router.address2twin, id)
                twin = router.address2twin[id]
            else
                @debug "creating anonymous twin from identity $id ($(bytes2zid(id)))"
                # create the twin
                twin = create_twin(string(bytes2zid(id)), router, zrouter)
                @debug "[anonymous] client bound to twin id [$twin]"
                router.address2twin[id] = twin
                router.twin2address[ucid(twin)] = id
                twin.zaddress = id
                twin.socket = router.zmqsocket
            end

            msg::RembusMsg = broker_parse(pkt, isa(router, Router))

            eval_message(twin, msg, id)
        catch e
            if isa(e, Visor.ProcessInterrupt) || isa(e, ZMQ.StateError)
                rethrow()
            end
            @warn "[ZMQ] recv error: $e"
            showerror(stdout, e, catch_backtrace())
            @showerror e
        end
    end
end

function identity_upgrade(router, twin, msg, id; authenticate=false)
    newtwin = attestation(router, twin, msg, authenticate)
    if newtwin !== nothing
        router.address2twin[id] = newtwin
        delete!(router.twin2address, ucid(twin))
        router.twin2address[ucid(newtwin)] = id
    end

    return nothing
end

function close_is_ok(ws::WebSockets.WebSocket, e)
    HTTP.WebSockets.isok(e)
end

function close_is_ok(ws::TCPSocket, e)
    isa(e, WrongTcpPacket)
end

close_is_ok(::Nothing, e) = true

#=
    detach(twin)

Disconnect the twin from the ws/tcp channel.
=#
function detach(twin)
    if twin.socket !== nothing
        if !isa(twin.socket, ZMQ.Socket)
            ## TBD: check if neeeded
            #flush(twin.socket.io)
            close(twin.socket)
        end
        twin.socket = nothing
    end
    twin.reactive = false
    return nothing
end

#=
    twin_task(self, twin)

Twin task that read messages from router and send to client.

It enqueues the input messages if the component is offline.
=#
function twin_task(self, twin)
    try
        twin.process = self
        @debug "starting twin [$(twin.id)]"
        for msg in self.inbox
            if isshutdown(msg)
                break
            elseif isa(msg, ResMsg)
                #@mlog("[$(twin.id)] -> $msg")
                transport_send(Val(twin.type), twin, msg, true)
            else
                signal!(twin, msg)
            end
        end
    catch e
        @error "[$twin] twin_task: $e" exception = (e, catch_backtrace())
        rethrow()
    finally
        if isa(twin.socket, WebSockets.WebSocket)
            close(twin.socket, WebSockets.CloseFrameBody(1008, "unexpected twin close"))
        end
    end
    # forward the message counter to the last message received when online
    # because these messages get already a chance to be delivered.
    if twin.reactive
        twin.mark = twin.router.mcounter
    end
    @debug "[$twin] task done"
end

#=
    handle_ack_timeout(tim, twin, msg, msgid)

Persist a PubSub message in case the acknowledge message is not received.
=#
function handle_ack_timeout(tim, twin, msg, msgid)
    delete!(twin.acktimer, msgid)

    if haskey(twin.out, msgid)
        put!(twin.out[msgid], false)
    end
end

function notreactive(twin, msg)
    twin.reactive === false && isa(msg.content, PubSubMsg)
end

#=
    signal!(twin, msg::Msg)

Send `msg` to client or enqueue it if it is offline.

Register the message into Twin.sent table.
=#
function signal!(twin, msg::Msg)
    @debug "[$twin] message>>: $msg, offline:$(offline(twin)), type:$(msg.ptype)"
    if (offline(twin) || notreactive(twin, msg)) && msg.ptype === TYPE_PUB
        return nothing
    end
    # current message
    if isa(msg.content, RpcReqMsg)
        tmr = Timer(request_timeout()) do tmr
            delete!(twin.sent, msg.content.id)
        end
        # add to sent table
        twin.sent[msg.content.id] = SentData(time(), msg, tmr)
    end

    if (msg.ptype === TYPE_PUB)
        msg_tmark = msg.counter
        twin_tmark = twin.mark
        if (msg_tmark < twin_tmark)
            # message already sent
            @debug "[$twin] msg $msg already sent: $(msg.content.data)"
            return nothing
        end
    end
    pkt = msg.content
    #@mlog "[$twin] -> $pkt"
    try
        # retry until outcome is true (ack message received)
        outcome = false
        while !outcome
            outcome = transport_send(Val(twin.type), twin, pkt)
        end
        if isa(pkt, PubSubMsg)
            twin.mark = msg.counter
        end
    catch e
        @error "[$twin] going offline: $e"
        @showerror e
        detach(twin)
    end
    return nothing
end

#=
    callback_or(fn::Function, router::AbstractRouter, callback::Symbol)

Invoke `callback` function if it is injected via the plugin module otherwise invoke `fn`.
=#
function callback_or(fn::Function, router::AbstractRouter, callback::Symbol)
    if router.plugin !== nothing && isdefined(router.plugin, callback)
        cb = getfield(router.plugin, callback)
        cb(router.shared, router)
    else
        fn()
    end
end

#=
    callback_and(fn, cb::Symbol, router::AbstractRouter, twin::Twin, msg::RembusMsg)

Get `cb` function and invoke it if is injected via the plugin module and then invoke `fn`.

If callback throws an error then `fn` is not called.

# Arguments

- `fn::Function`: the function to invoke anyway if `cb` does not throw.
- `cb::Symbol`: the name of the method defined in the external plugin
- `router::AbstractRouter`: the instance of the broker router
- `twin::Twin`: the target twin
- `msg::RembusMsg`: the message to handle
=#
function callback_and(
    fn::Function, cb::Symbol, router::AbstractRouter, twin::Twin, msg::RembusMsg
)
    try
        if router.plugin !== nothing && isdefined(router.plugin, cb)
            cb = getfield(router.plugin, cb)
            cb(router.shared, router, twin, msg)
        end
        fn()
    catch e
        @error "$cb callback error: $e"
    end
end

function command_line(default_name="broker")
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--name", "-n"
        help = "broker name"
        default = default_name
        arg_type = String
        "--reset", "-r"
        help = "factory reset, clean up broker configuration"
        action = :store_true
        "--secure", "-s"
        help = "accept wss and tls connections"
        action = :store_true
        "--http", "-p"
        help = "accept HTTP clients on port HTTP"
        arg_type = UInt16
        "--ws", "-w"
        help = "accept WebSocket clients on port WS"
        arg_type = UInt16
        "--tcp", "-t"
        help = "accept tcp clients on port TCP"
        arg_type = UInt16
        "--zmq", "-z"
        help = "accept zmq clients on port ZMQ"
        arg_type = UInt16
        "--policy"
        help = "set the broker routing policy"
        arg_type = Symbol
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
    end
    return parse_args(s)
end

function caronte_reset(broker_name="broker")
    rm(messages_dir(broker_name), force=true, recursive=true)
    bdir = broker_dir(broker_name)
    if isdir(bdir)
        foreach(rm, filter(isfile, readdir(bdir, join=true)))
    end
end

#=
    If value is nothing returns the value of the args dictionary key.
=#
function getparam(args, key, value)
    if value !== nothing
        return value
    else
        return get(args, key, nothing)
    end
end

"""
    broker(;
        wait=true,
        secure=nothing,
        ws=nothing,
        tcp=nothing,
        zmq=nothing,
        http=nothing,
        name="broker",
        policy=:first_up,
        mode=nothing,
        reset=nothing,
        log="info",
        plugin=nothing,
        context=nothing
    )

Start the broker.

Return immediately when `wait` is false, otherwise blocks until shutdown is requested.

Overwrite command line arguments if args is not empty.
"""
function broker(;
    wait=true,
    secure=nothing,
    ws=nothing,
    tcp=nothing,
    zmq=nothing,
    http=nothing,
    name="broker",
    policy=:first_up,
    mode=nothing,
    reset=nothing,
    log=TRACE_INFO,
    plugin=nothing,
    context=nothing
)
    args = command_line()

    sv_name = getparam(args, "name", name)
    setup(CONFIG)
    CONFIG.log_level = String(log)

    if haskey(args, "debug") && args["debug"] === true
        CONFIG.log_level = TRACE_DEBUG
    end
    rst = getparam(args, "reset", reset)
    if rst
        Rembus.caronte_reset(sv_name)
    end

    issecure = getparam(args, "secure", secure)
    router = Router(sv_name, plugin, context)

    policy = Symbol(getparam(args, "policy", policy))
    if !(policy in [:first_up, :less_busy, :round_robin])
        error("wrong broker policy, must be one of :first_up, :less_busy, :round_robin")
    end
    router.policy = policy

    if mode !== nothing
        router.mode = string_to_enum(mode)
    end

    tasks = [
        process("broker", broker_task, args=(router,)),
        supervisor("twins", terminateif=:shutdown)
    ]

    http_port = getparam(args, "http", http)
    if http_port !== nothing
        router.listeners[:http] = Listener(http_port)
        push!(
            tasks,
            process(
                serve_http,
                args=(router, http_port, issecure),
                restart=:transient
            )
        )
    end

    tcp_port = getparam(args, "tcp", tcp)
    if tcp_port !== nothing
        router.listeners[:tcp] = Listener(tcp_port)
        push!(
            tasks,
            process(
                serve_tcp,
                args=(router, tcp_port, issecure),
                restart=:transient
            )
        )
    end

    zmq_port = getparam(args, "zmq", zmq)
    if zmq_port !== nothing
        router.listeners[:zmq] = Listener(zmq_port)
        push!(
            tasks,
            process(
                serve_zmq,
                args=(router, zmq_port),
                restart=:transient,
                debounce_time=2)
        )
    end

    ws_port = getparam(args, "ws", ws)
    if ws_port !== nothing || (zmq_port === nothing && tcp_port === nothing)
        if ws_port === nothing
            ws_port = parse(UInt16, get(ENV, "BROKER_WS_PORT", "8000"))
        end

        router.listeners[:ws] = Listener(ws_port)
        push!(
            tasks,
            process(
                serve_ws,
                args=(router, ws_port, issecure),
                restart=:transient,
                stop_waiting_after=2.0)
        )
    end
    supervise(
        [supervisor(sv_name, tasks, strategy=:one_for_all, intensity=1)],
        wait=wait,
        intensity=2
    )

    yield()
    return router
end

function brokerd()::Cint
    broker()
    return 0
end

version(ctx, rb) = version()
version() = VERSION

"""
    server(
        ctx=nothing;
        secure=false,
        ws=nothing,
        tcp=nothing,
        http=nothing,
        zmq=nothing,
        name="server",
        mode=nothing,
        log=TRACE_INFO
    )

Initialize a server node.
"""
function server(
    shared=missing;
    secure=false,
    ws=nothing,
    tcp=nothing,
    http=nothing,
    zmq=nothing,
    name="server",
    mode=nothing,
    log=TRACE_INFO
)
    args = command_line(name)
    name = getparam(args, "name", name)
    rb = Server(name, shared)
    expose(rb, "version", version)

    ws_port = getparam(args, "ws", ws)
    issecure = getparam(args, "secure", secure)

    setup(CONFIG)
    if mode !== nothing
        rb.mode = string_to_enum(mode)
    end

    acceptors = []

    http_port = getparam(args, "http", http)
    if http_port !== nothing
        rb.listeners[:http] = Listener(http_port)
        push!(
            acceptors,
            process(
                "serve_http:$http_port",
                serve_http,
                args=(rb, http_port, issecure),
                restart=:transient
            )
        )
    end

    tcp_port = getparam(args, "tcp", tcp)
    if tcp_port !== nothing
        rb.listeners[:tcp] = Listener(tcp_port)
        push!(
            acceptors,
            process(
                "serve_tcp:$tcp_port",
                serve_tcp,
                args=(rb, tcp_port, issecure),
                restart=:transient
            )
        )
    end

    zmq_port = getparam(args, "zmq", zmq)
    if zmq_port !== nothing
        rb.listeners[:zmq] = Listener(zmq_port)
        push!(
            acceptors,
            process(
                "serve_zmq:$zmq_port",
                serve_zmq,
                args=(rb, zmq_port),
                restart=:transient,
                debounce_time=2
            )
        )
    end

    ws_port = getparam(args, "ws", ws)
    if ws_port !== nothing || (tcp_port === nothing && zmq_port === nothing)
        if ws_port === nothing
            ws_port = parse(UInt16, get(ENV, "BROKER_WS_PORT", "8000"))
        end

        rb.listeners[:ws] = Listener(ws_port)
        push!(
            acceptors,
            process(
                "serve:$ws_port",
                serve_ws,
                args=(rb, ws_port, issecure),
                restart=:transient,
                stop_waiting_after=2.0
            )
        )
    end

    embedded_sv = from(name)
    if embedded_sv === nothing
        # first rb process
        CONFIG.log_level = String(log)

        if haskey(args, "debug") && args["debug"] === true
            CONFIG.log_level = TRACE_DEBUG
        end

        init_log()
        tasks = Visor.Supervised[supervisor("twins", terminateif=:shutdown)]
        append!(tasks, acceptors)
        supervise(
            [supervisor(name, tasks, strategy=:one_for_one)],
            intensity=5,
            wait=false
        )
    else
        for acceptor in acceptors
            Visor.add_node(embedded_sv, acceptor)
            Visor.start(acceptor)
        end
    end
    rb.process = from(name)
    rb.owners = load_tenants(rb)
    rb.component_owner = load_token_app(rb)

    return rb
end

"""
    forever(server::Server; wait=true, secure=false)

Start an embedded server and accept connections.
"""
function forever(rb::Server)

    if !isinteractive()
        wait(Visor.root_supervisor(rb.process))
    end

    return nothing
end

function router_configuration(router)
    cfg = Dict("exposers" => Dict(), "subscribers" => Dict())
    for (topic, twins) in router.topic_impls
        cfg["exposers"][topic] = [t.id for t in twins]
    end
    for (topic, twins) in router.topic_interests
        cfg["subscribers"][topic] = [t.id for t in twins]
    end

    return cfg
end

prettystr(msg::RembusMsg) = "RembusMsg: $msg"

function prettystr(msg::PubSubMsg)
    if isa(msg.data, Vector{UInt8})
        len = length(msg.data)
        cnt = len > 10 ? msg.data[1:10] : msg.data
        return "binary[$len] $cnt ..."
    else
        return msg.topic
    end
end

#=
Entry point of a new connection request from a node.
=#
function client_receiver(router::Router, sock)
    uid = uuid4()
    cid = string(uid)
    twin = create_twin(cid, router, socket)
    @debug "[anonymous] client bound to twin id [$cid]"
    # start the twin client task
    twin.socket = sock

    if router.mode === authenticated
        @debug "only authenticated nodes allowed"
        response = challenge(router, twin, CONNECTION_ID)
        put!(twin.process.inbox, response)
        authtwin = authenticate_twin_receiver(router, twin)
    else
        # ws/tcp socket receiver task
        authtwin = anonymous_twin_receiver(router, twin)
    end

    # upgrade receiver if twin is related to a named or authenticated node
    if (authtwin !== nothing)
        twin_receiver(router, authtwin)
    end

    return nothing
end

#=
Returns
 * .sts=STS_SUCCESS, .value=Twin or RbServerConnection for a named but unauth node
 * .sts=STS_GENERIC_ERROR, .value=nothing for an error
 * .sts=STS_CHALLENGE, .value=nothing when a Challenge packet is sent
=#
function identity_check(router, twin, msg; paging=true)::IdentityReturn
    @debug "[$twin] auth identity: $(msg.cid)"
    if isempty(msg.cid)
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "empty cid"), twin)
        return IdentityReturn(STS_GENERIC_ERROR, nothing)
    end
    named = named_twin(msg.cid, router)
    if named !== nothing && !offline(named)
        @warn "a component with id [$(msg.cid)] is already connected"
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "already connected"), twin)
    else
        # check if cid is registered
        if key_file(router, msg.cid) !== nothing
            # authentication mode, send the challenge
            response = challenge(router, twin, msg.id)
            respond(router, response, twin)
            return IdentityReturn(STS_CHALLENGE, nothing)
        else
            authtwin = setidentity(router, twin, msg, paging=paging)
            respond(router, ResMsg(msg.id, STS_SUCCESS, nothing), authtwin)
            return IdentityReturn(STS_SUCCESS, authtwin)
        end
    end
    return IdentityReturn(STS_GENERIC_ERROR, nothing)
end

function client_receiver(router::Server, ws)
    id = string(uuid4())
    rb = RBServerConnection(router, id, socket)
    rb.socket = ws
    push!(router.connections, rb)
    spec = process(id, server_task, args=(rb,))
    sv = router.process
    p = startup(Visor.from_supervisor(sv, "twins"), spec)
    router.id_twin[id] = rb
    read_socket(ws, p, rb)
    delete!(router.id_twin, id)
    return nothing
end

function secure_config(router)
    trust_store = keystore_dir(router)

    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)

    sslconfig = MbedTLS.SSLConfig(
        joinpath(trust_store, "rembus.crt"),
        joinpath(trust_store, "rembus.key")
    )
    MbedTLS.rng!(sslconfig, rng)

    function show_debug(level, filename, number, msg)
        @show level, filename, number, msg
    end

    MbedTLS.dbg!(sslconfig, show_debug)
    return sslconfig
end

function listener(proc, caronte_port, router, sslconfig)
    IP = "0.0.0.0"
    server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
    router.ws_server = server
    proto = (sslconfig === nothing) ? "ws" : "wss"
    @info "$(proc.supervisor) listening at port $proto:$caronte_port"

    setphase(proc, :listen)

    return HTTP.WebSockets.listen!(
        IP,
        caronte_port,
        server=server,
        sslconfig=sslconfig,
        verbose=-1
    ) do ws
        client_receiver(router, ws)
    end
end

function verify_basic_auth(router, authorization)
    # See: https://datatracker.ietf.org/doc/html/rfc7617
    val = String(Base64.base64decode(replace(authorization, "Basic " => "")))
    idx = findfirst(':', val)

    if idx === nothing
        # no password, only component name
        cid = val
        if key_file(router, cid) !== nothing || CONFIG.connection_mode === authenticated
            error("$cid: authentication failed")
        end
    else
        cid = val[1:idx-1]
        pwd = val[idx+1:end]
        file = key_file(router, cid)
        if file !== nothing
            secret = readline(file)
            if secret != pwd
                error("$cid: authentication failed")
            end
        else
            error("$cid: authentication failed")
        end
    end
    return cid
end

function authenticate(router::AbstractRouter, req::HTTP.Request)
    auth = HTTP.header(req, "Authorization")

    if auth !== ""
        cid = verify_basic_auth(router, auth)
    else
        if CONFIG.connection_mode === authenticated
            error("anonymous: authentication failed")
        else
            cid = string(uuid4())
        end
    end

    return cid
end

function authenticate_admin(router::AbstractRouter, req::HTTP.Request)
    cid = authenticate(router, req)
    if !(cid in router.admins)
        error("$cid authentication failed")
    end

    return cid
end

function http_admin_msg(router, twin, msg)
    admin_res = admin_command(router, twin, msg)
    @debug "http admin response: $admin_res"
    return admin_res
end

function command(router::Router, req::HTTP.Request, cmd::Dict)
    sts = 403
    cid = HTTP.getparams(req)["cid"]
    topic = HTTP.getparams(req)["topic"]
    twin = create_twin(cid, router, loopback)
    twin.hasname = true
    msg = AdminReqMsg(topic, cmd)
    response = http_admin_msg(router, twin, msg)
    if response.status == 0
        sts = 200
    end
    return HTTP.Response(sts, [])
end

function http_subscribe(router::AbstractRouter, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => SUBSCRIBE_CMD))
end

function http_unsubscribe(router::AbstractRouter, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNSUBSCRIBE_CMD))
end

function http_expose(router::AbstractRouter, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => EXPOSE_CMD))
end

function http_unexpose(router::AbstractRouter, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNEXPOSE_CMD))
end

function http_publish(server::Server, req::HTTP.Request)
    try
        cid = authenticate(server, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end

        if haskey(server.topic_function, topic)
            server.topic_function[topic](content...)
        end

        return HTTP.Response(200, [])
    catch e
        @info "http::publish: $e"
        return HTTP.Response(403, [])
    end
end

function http_publish(router::AbstractRouter, req::HTTP.Request)
    try
        cid = authenticate(router, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end
        twin = create_twin(cid, router, loopback)
        msg = PubSubMsg(topic, content)
        pubsub_msg(router, twin, msg)
        Visor.shutdown(twin.process)
        cleanup(twin, router)
        return HTTP.Response(200, [])
    catch e
        @info "http::publish: $e"
        return HTTP.Response(403, [])
    end
end

function http_rpc(server::Server, req::HTTP.Request)
    try
        authenticate(server, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end

        sts = 403
        retval = []
        if haskey(server.topic_function, topic)
            retval = server.topic_function[topic](content...)
            sts = 200
        end

        return HTTP.Response(
            sts,
            ["Content_type" => "application/json"],
            JSON3.write(retval)
        )
    catch e
        @info "http::rpc: $e"
        return HTTP.Response(404, [])
    end
end

function http_rpc(router::Router, req::HTTP.Request)
    try
        cid = authenticate(router, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end
        if haskey(router.id_twin, cid)
            error("component $cid not available for rpc via http")
        else
            twin = create_twin(cid, router, loopback)
            twin.socket = Condition()
            msg = RpcReqMsg(topic, content)
            rpc_request(router, twin, msg)
            response = wait(twin.socket)
            if isa(response.data, IOBuffer)
                retval = decode(response.data)
            else
                retval = response.data
            end

            if response.status == 0
                sts = 200
            else
                sts = 403
            end

            Visor.shutdown(twin.process)
            cleanup(twin, router)
            return HTTP.Response(
                sts,
                ["Content_type" => "application/json"],
                JSON3.write(retval)
            )
        end
    catch e
        @error "http::rpc: $e"
        return HTTP.Response(404, [])
    end
end

function http_admin_command(
    router::AbstractRouter, req::HTTP.Request, cmd::Dict, topic="__config__"
)
    try
        cid = authenticate_admin(router, req)
        twin = create_twin(cid, router, loopback)
        msg = AdminReqMsg(topic, cmd)
        response = http_admin_msg(router, twin, msg)
        Visor.shutdown(twin.process)
        if response.status === STS_SUCCESS
            if response.data !== nothing
                return HTTP.Response(200, JSON3.write(response.data))
            else
                return HTTP.Response(200, [])
            end
        else
            return HTTP.Response(403, [])
        end
    catch e
        @error "http::admin: $e"
        return HTTP.Response(403, [])
    end
end

function http_admin_command(router::AbstractRouter, req::HTTP.Request)
    try
        authenticate_admin(router, req)
        cmd = HTTP.getparams(req)["cmd"]
        return http_admin_command(
            router, req, Dict(COMMAND => cmd)
        )
    catch e
        @error "http::admin: $e"
        return HTTP.Response(403, [])
    end

end

function http_private_topic(router::AbstractRouter, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(router, req, Dict(COMMAND => PRIVATE_TOPIC_CMD), topic)
end

function http_public_topic(router::AbstractRouter, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(router, req, Dict(COMMAND => PUBLIC_TOPIC_CMD), topic)
end

function http_authorize(router::AbstractRouter, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(
        router,
        req,
        Dict(COMMAND => AUTHORIZE_CMD, CID => HTTP.getparams(req)["cid"]),
        topic
    )
end

function http_unauthorize(router::AbstractRouter, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(
        router,
        req,
        Dict(COMMAND => UNAUTHORIZE_CMD, CID => HTTP.getparams(req)["cid"]),
        topic
    )
end

function body(response::HTTP.Response)
    if isempty(response.body)
        return nothing
    else
        return JSON3.read(response.body, Any)
    end
end

function _serve_http(td, router::AbstractRouter, http_router, port, issecure)
    try
        router.listeners[:http].status = on
        sslconfig = nothing
        if issecure
            sslconfig = secure_config(router)
        end

        router.http_server = HTTP.serve!(
            http_router, ip"0.0.0.0", port, sslconfig=sslconfig
        )
        for msg in td.inbox
            if isshutdown(msg)
                break
            end
        end
    catch e
        @error "[serve_http] error: $e"
    finally
        @info "[serve_http] closed"
        router.listeners[:http].status = off
        setphase(td, :terminate)
        isdefined(router, :http_server) && close(router.http_server)
    end
end

function serve_http(td, router::Server, port, issecure=false)
    @info "[serve_http] starting at port $port"

    # define REST endpoints to dispatch rembus functions
    http_router = HTTP.Router()
    # publish
    HTTP.register!(http_router, "POST", "{topic}", req -> http_publish(router, req))
    # rpc
    HTTP.register!(http_router, "GET", "{topic}", req -> http_rpc(router, req))

    _serve_http(td, router, http_router, port, issecure)
end

function serve_http(td, router::Router, port, issecure=false)
    @info "[serve_http] starting at port $port"

    # define REST endpoints to dispatch rembus functions
    http_router = HTTP.Router()
    # publish
    HTTP.register!(http_router, "POST", "{topic}", req -> http_publish(router, req))
    # rpc
    HTTP.register!(http_router, "GET", "{topic}", req -> http_rpc(router, req))
    # admin
    HTTP.register!(http_router,
        "GET", "admin/{cmd}",
        req -> http_admin_command(router, req)
    )

    HTTP.register!(http_router,
        "POST",
        "subscribe/{topic}/{cid}",
        req -> http_subscribe(router, req)
    )
    HTTP.register!(http_router,
        "POST",
        "unsubscribe/{topic}/{cid}",
        req -> http_unsubscribe(router, req)
    )
    HTTP.register!(http_router,
        "POST",
        "expose/{topic}/{cid}",
        req -> http_expose(router, req)
    )
    HTTP.register!(http_router,
        "POST",
        "unexpose/{topic}/{cid}",
        req -> http_unexpose(router, req)
    )

    HTTP.register!(
        http_router,
        "POST",
        "private_topic/{topic}",
        req -> http_private_topic(router, req)
    )
    HTTP.register!(
        http_router,
        "POST",
        "public_topic/{topic}",
        req -> http_public_topic(router, req)
    )
    HTTP.register!(
        http_router,
        "POST",
        "/authorize/{cid}/{topic}",
        req -> http_authorize(router, req)
    )
    HTTP.register!(
        http_router,
        "POST",
        "/unauthorize/{cid}/{topic}",
        req -> http_unauthorize(router, req)
    )

    _serve_http(td, router, http_router, port, issecure)
end

function router_ready(router)
    while isnan(router.start_ts)
        sleep(0.05)
    end
end

function serve_ws(td, router, port, issecure=false)
    @debug "[serve_ws] starting"
    router_ready(router)
    router.listeners[:ws].status = on

    sslconfig = nothing
    try
        if issecure
            sslconfig = secure_config(router)
        end

        listener(td, port, router, sslconfig)
        for msg in td.inbox
            if isshutdown(msg)
                return
            end
        end
    catch e
        if !isa(e, Visor.ProcessInterrupt)
            @error "[serve_ws]: $e"
            @showerror e
        end
        rethrow()
    finally
        @debug "[serve_ws] closed"
        router.listeners[:ws].status = off
        setphase(td, :terminate)
        isdefined(router, :ws_server) && close(router.ws_server)
    end
end

function serve_zmq(pd, router, port)
    @debug "[serve_zmq] starting"
    router_ready(router)
    router.zmqcontext = ZMQ.Context()
    router.zmqsocket = Socket(router.zmqcontext, ROUTER)
    ZMQ.bind(router.zmqsocket, "tcp://*:$port")
    router.listeners[:zmq].status = on
    try
        @info "$(pd.supervisor) listening at port zmq:$port"
        setphase(pd, :listen)
        zeromq_receiver(router)
    catch e
        # consider ProcessInterrupt a normal termination because
        # zeromq_receiver is not polling for supervisor shutdown message
        if !isa(e, Visor.ProcessInterrupt)
            @error "[serve_zmq] error: $e"
            rethrow()
        end
    finally
        router.listeners[:zmq].status = off
        setphase(pd, :terminate)
        ZMQ.close(router.zmqsocket)
        ZMQ.close(router.zmqcontext)
        @debug "[serve_zmq] closed"
    end
end

function serve_tcp(pd, router, caronte_port, issecure=false)
    router_ready(router)
    proto = "tcp"
    server = nothing
    try
        IP = "0.0.0.0"
        if issecure
            proto = "tls"
            sslconfig = secure_config(router)
        end

        server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
        router.server = server
        router.listeners[:tcp].status = on

        @info "$(pd.supervisor) listening at port $proto:$caronte_port"
        setphase(pd, :listen)
        while true
            sock = accept(server)
            if issecure
                ctx = MbedTLS.SSLContext()
                MbedTLS.setup!(ctx, sslconfig)
                MbedTLS.associate!(ctx, sock)
                MbedTLS.handshake(ctx)
                @async client_receiver(router, ctx)
            else
                @async client_receiver(router, sock)
            end
        end
    finally
        router.listeners[:tcp].status = off
        setphase(pd, :terminate)
        server !== nothing && close(server)
    end
end

function islistening(
    ; wait=5, procs=["broker.serve_ws", "broker.serve_tcp", "broker.serve_zmq"]
)
    tcount = 0
    while tcount < wait
        if all(p -> (p !== nothing) && (getphase(p) === :listen), from.(procs))
            return true
        end
        tcount += 0.2
        sleep(0.2)
    end

    return false
end

function islistening(router::AbstractRouter; protocol::Vector{Symbol}=[:ws], wait=0)
    while wait >= 0
        all_listening = true
        for p in protocol
            if !haskey(router.listeners, p)
                return false
            elseif router.listeners[p].status === off
                all_listening = false
            end
        end
        all_listening && break
        wait -= 0.2
        sleep(0.2)
    end

    return (wait >= 0)
end

isconnected(twin) = twin.socket !== nothing && isopen(twin.socket)

function first_up(router, topic, implementors)
    @debug "[$topic] first_up routing policy"
    for target in implementors
        @debug "[$topic] candidate target: $target"
        if isconnected(target)
            return target
        end
    end

    return nothing
end

function round_robin(router, topic, implementors)
    target = nothing
    if !isempty(implementors)
        len = length(implementors)
        @debug "[$topic]: $len implementors"
        current_index = get(router.last_invoked, topic, 0)
        if current_index === 0
            while (target === nothing) && (current_index < len)
                current_index += 1
                impl = implementors[current_index]
                !isconnected(impl) && continue
                target = impl
                router.last_invoked[topic] = current_index
            end
        else
            cursor = 1
            current_index = current_index >= len ? 1 : current_index + 1
            for impl in implementors
                if current_index > cursor
                    if target === nothing && isconnected(impl)
                        target = impl
                        router.last_invoked[topic] = cursor
                    end
                    cursor += 1
                else
                    if isconnected(impl)
                        target = impl
                        router.last_invoked[topic] = cursor
                        break
                    else
                        cursor += 1
                    end
                end
            end
        end
    end

    return target
end

Base.isless(t1::Twin, t2::Twin) = length(t1.sent) < length(t2.sent)

function less_busy(router, topic, implementors)
    up_and_running = [impl for impl in implementors if isconnected(impl)]
    if isempty(up_and_running)
        return nothing
    else
        return min(up_and_running...)
    end
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

#=
    broadcast!(router, msg)

Broadcast the `topic` data `msg` to all interested clients.
=#
function broadcast!(router, msg)
    authtwins = Set{Twin}()
    if msg.ptype == TYPE_PUB
        # the interest * (subscribe to all topics) is enabled
        # only for pubsub messages and not for rpc methods.
        topic = msg.content.topic
        bmsg = msg.content
        twins = get(router.topic_interests, "*", Set{Twin}())
        # broadcast to twins that are admins and to twins that are authorized to
        # subscribe to topic
        for twin in twins
            if twin.id in router.admins
                push!(authtwins, twin)
            elseif haskey(router.topic_auth, topic)
                if haskey(router.topic_auth[topic], twin.id)
                    # it is a private topic, check if twin is authorized
                    push!(authtwins, twin)
                end
            else
                # it is a public topic, all twins may be broadcasted
                push!(authtwins, twin)
            end
        end
    elseif isdefined(msg, :reqdata)
        topic = msg.reqdata.topic
        bmsg = PubSubMsg(topic, msg.reqdata.data)
        msg = Msg(TYPE_PUB, bmsg, msg.twchannel, UInt64(1))
    else
        @debug "no broadcast for [$msg]: request data not available or server method"
        return nothing
    end

    union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))

    for tw in authtwins
        @debug "broadcasting $topic to $tw"
        put!(tw.process.inbox, msg)
    end

    return nothing
end

"""
    isauthenticated(session)

Return true if the connected component is authenticated.
"""
isauthenticated(session) = session.isauth

#=
    isauthorized(router::Router, twin::Twin, topic::AbstractString)

Return true if topic is public or client is authorized to bind to topic.
=#
function isauthorized(router::AbstractRouter, twin::Twin, topic::AbstractString)
    # check if topic is private
    if haskey(router.topic_auth, topic)
        # check if twin is authorized to bind to topic
        if !haskey(router.topic_auth[topic], twin.id)
            return false
        end
    end

    # topic is public or twin is authorized
    return true
end

#=
    isadmin(router, twin, cmd)
Check if twin client has admin privilege.
=#
function isadmin(router, twin, cmd)
    sts = twin.id in router.admins
    if !sts
        @error "$cmd failed: $(twin.id) not authorized"
    end

    return sts
end

function respond(router::Router, msg::Msg)
    put!(msg.twchannel.process.inbox, msg)

    if msg.content.status != STS_SUCCESS
        return
    end

    # broadcast! to all interested twins
    broadcast!(router, msg)

    return nothing
end

respond(::AbstractRouter, msg::RembusMsg, twin) = put!(twin.process.inbox, msg)

function uptime(router)
    utime = time() - router.start_ts
    return "up for $(Int(floor(utime))) seconds"
end

function getargs(data)
    if isa(data, ZMQ.Message)
        args = decode(Vector{UInt8}(data))
    else
        args = data
    end
    if args isa Vector
        return args
    elseif args === nothing
        return []
    else
        return [args]
    end
end

function embedded_eval(router, twin::Twin, msg::RembusMsg)
    result = nothing
    sts = STS_GENERIC_ERROR
    if haskey(router.topic_function, msg.topic)
        if isa(msg.data, ZMQ.Message)
            payload = msg.data
        else
            if isa(msg.data, Base.GenericIOBuffer)
                payload = dataframe_if_tagvalue(decode(msg.data))
            else
                payload = msg.data
            end
        end
        try
            result = router.topic_function[msg.topic](router.shared, twin, getargs(payload)...)
            sts = STS_SUCCESS
        catch e
            ##    @debug "[$(msg.topic)] server error (method too young?): $e"
            ##    result = "$e"
            ##    sts = STS_METHOD_EXCEPTION
            ##
            ##    if isa(e, MethodError)
            ##        try
            ##            result = Base.invokelatest(
            ##                router.topic_function[msg.topic],
            ##                router.shared,
            ##                twin,
            ##                getargs(payload)...
            ##            )
            ##            sts = STS_SUCCESS
            ##        catch e
            result = "$e"
            ##        end
            ##    end
        end

        if sts != STS_SUCCESS
            @error "[$(msg.topic)]: $result"
        end

        return (true, isa(msg, RpcReqMsg) ? ResMsg(msg, sts, result) : nothing)
    else
        return (false, nothing)
    end
end

function caronte_embedded_method(router, twin::Twin, msg::RembusMsg)
    (found, resmsg) = embedded_eval(router, twin, msg)

    if found
        if isa(resmsg, ResMsg)
            response = Msg(TYPE_RESPONSE, resmsg, twin)
            respond(router, response)
        end
    end

    return found
end

function encode_message(msg::PubSubMsg)
    io = IOBuffer()
    if isa(msg.data, ZMQ.Message)
        data = decode(Vector{UInt8}(msg.data))
    else
        data = msg.data
    end
    if msg.flags > QOS0
        encode_partial(io, [TYPE_PUB | msg.flags, id2bytes(msg.id), msg.topic, data])
    else
        encode_partial(io, [TYPE_PUB | msg.flags, msg.topic, data])
    end
    return take!(io)
end

#=
    broker_task(self, router)

Rembus broker main task.
=#
function broker_task(self, router)
    @debug "[broker] starting"

    try
        router.process = self
        init(router)

        # example for registering a broker implementor
        router.topic_function["uptime"] = (ctx, session) -> uptime(router)
        router.topic_function["version"] = (ctx, session) -> Rembus.VERSION

        for msg in self.inbox
            !isshutdown(msg) || break

            if isa(msg, Msg)
                @debug "[broker] recv [type=$(msg.ptype)]: $msg"
                if msg.ptype == TYPE_PUB
                    if CONFIG.save_messages
                        msg.counter = save_message(router, msg.content)
                    end
                    # publish to interested twins
                    broadcast!(router, msg)
                elseif msg.ptype == TYPE_RPC
                    topic = msg.content.topic
                    if caronte_embedded_method(router, msg.twchannel, msg.content)
                    else
                        # find an implementor
                        if haskey(router.topic_impls, topic)
                            # request a method exec
                            implementors = router.topic_impls[topic]
                            target = select_twin(router, topic, implementors)
                            @debug "[broker] exposer for $topic: [$target]"
                            if target === nothing
                                msg.content = ResMsg(msg.content, STS_METHOD_UNAVAILABLE, "$topic: method unavailable")
                                put!(msg.twchannel.process.inbox, msg)
                            elseif target.process.inbox === msg.twchannel.process.inbox
                                @warn "[$(target.id)]: loopback detected"
                                msg.content = ResMsg(msg.content, STS_METHOD_LOOPBACK, "$topic: method loopback")
                                put!(msg.twchannel.process.inbox, msg)
                            elseif target !== nothing
                                put!(target.process.inbox, msg)
                            end
                        else
                            # remote method not found
                            msg.content = ResMsg(msg.content, STS_METHOD_NOT_FOUND, "$topic: method unknown")
                            put!(msg.twchannel.process.inbox, msg)
                        end
                    end
                elseif msg.ptype == TYPE_RESPONSE
                    # it is a result from an exposer
                    # reply toward the client that has made the request
                    respond(router, msg)
                elseif msg.ptype == REACTIVE_MESSAGE
                    #if msg.twchannel.hasname
                    response = ResMsg(msg.content.id, STS_SUCCESS, nothing)
                    put!(msg.twchannel.process.inbox, response)
                    start_reactive(msg.twchannel, msg.content.msg_from)
                end
            else
                @warn "unknown message: $msg"
            end
        end
    catch e
        @error "[broker] error: $e"
        @showerror e
        rethrow()
    finally
        save_configuration(router)
        if CONFIG.save_messages
            persist_messages(router)
        end
        filter!(router.id_twin) do (id, tw)
            cleanup(tw, router)
            return true
        end
        @info "$(router.process.supervisor) shutted down"
    end
end

#=
    boot(router)

Setup the router.
=#
function boot(router)
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
    return nothing
end

function init_log()
    if !haskey(ENV, "JULIA_DEBUG")
        logging()
    end
end

function init(router)
    init_log()
    boot(router)
    @debug "broker datadir: $(broker_dir(router))"

    if router.plugin !== nothing
        if isdefined(router.plugin, :twin_initialize)
            router.twin_initialize = getfield(router.plugin, :twin_initialize)
        end
        if isdefined(router.plugin, :twin_finalize)
            router.twin_finalize = getfield(router.plugin, :twin_finalize)
        end

        topics = names(router.plugin)
        exposed = filter(
            sym -> isa(sym, Function),
            [getfield(router.plugin, t) for t in topics]
        )
        for topic in exposed
            router.topic_function[string(topic)] = topic
        end
        if isdefined(router.plugin, :publish_interceptor)
            router.pub_handler = getfield(router.plugin, :publish_interceptor)
        end
    end

    return nothing
end

function response_timeout(condition::Distributed.Future, msg::RembusMsg)
    descr = "[$msg]: request timeout"
    put!(condition, RembusTimeout(descr))
    return nothing
end

#=
Send a request to a remote component.

For example the IdentityMsg request is initiated by the twin
when a broker connects to a component that acts as an Acceptor
=#
function wait_response(twin::Twin, msg::Msg, timeout)
    mid::UInt128 = msg.content.id
    resp_cond = Distributed.Future()
    twin.out[mid] = resp_cond
    t = Timer((tim) -> response_timeout(resp_cond, msg.content), timeout)
    push!(twin.process.inbox, msg)
    res = fetch(resp_cond)
    close(t)
    delete!(twin.out, mid)
    if isa(res, Exception)
        throw(res)
    end
    return res
end

#=
    add_node(components)

Add a server.
=#
function add_node(router, component)
    url = cid(component)
    if !(url in router.servers)
        push!(router.servers, url)
        connect(router, component)
    end
end

add_node(router, url::AbstractString) = add_node(router, RbURL(url))

#=
    remove_node(components)

Remove a server.
=#
function remove_node(router, component)
    url = cid(component)
    proc = from_name(url)
    if proc !== nothing
        shutdown(proc)
    end
    delete!(router.servers, url)
end

remove_node(router, url::AbstractString) = remove_node(router, RbURL(url))

function setup_receiver(process, socket, twin::Twin, isconnected)
    twin.socket = socket
    notify(isconnected)
    twin_receiver(twin.router, twin)
    put!(process.inbox, "connection closed")
end

brokerurl(rb::Twin) = brokerurl(RbURL(rb.id))

protocol(rb::Twin) = RbURL(rb.id).protocol

#=
Broker task that establishes the connection to servers and brokers.
=#
function egress_task(proc, twin::Twin, remote::RbURL)
    _connect(twin, proc)
    if hasname(twin)
        msg = IdentityMsg(RbURL(twin.id).id)
        wait_response(twin, Msg(TYPE_IDENTITY, msg, twin), request_timeout())
    end

    msg = take!(proc.inbox)
    if isshutdown(msg)
        close(twin.socket)
    else
        # the only message is an error condition
        error(msg)
    end
    @debug "[$proc] connect to broker done"
end

#=
Connect to server or broker extracted from remote_url.

Loopback connection is not permitted.
=#
function connect(
    router::Router, remote_url::AbstractString
)
    return connect(router, RbURL(remote_url))
end

function connect(
    router::Router, remote::RbURL
)
    # setup the twin
    remote_url = cid(remote)

    remote_ip = getaddrinfo(remote.host)
    listener = get(router.listeners, remote.protocol, Listener(0))
    for ip in [getipaddrs(); [ip"127.0.0.1"]]
        if ip == remote_ip && remote.port == listener.port
            error("remote component $(remote.id): loopback not permitted")
        end
    end

    # TODO: test ZMQ and TCP connections
    twin = create_twin(remote_url, router, nodetype(remote))
    twin.hasname = hasname(remote)

    # start the process which setup the connection with remote acceptor.
    startup(
        router.process.supervisor.supervisor,
        process(remote_url, egress_task, args=(twin, remote), debounce_time=6)
    )

    return nothing
end
