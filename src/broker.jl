#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

@enum QoS fast with_ack

struct EnableReactiveMsg
    id::UInt128
end

abstract type AbstractRouter end

mutable struct Pager
    io::Union{Nothing,IOBuffer}
    ts::Int # epoch time
    size::UInt # approximate page size
    Pager() = new(nothing, Libc.TimeVal().sec, CONFIG.page_size)
    Pager(io::IOBuffer, ts=Libc.TimeVal().sec) = new(io, ts, CONFIG.page_size)
end

#=
Twin is the broker-side image of a component.

`sock` is the socket handle when the protocol is tcp/tls or ws/wss.

For ZMQ sockets one socket is shared between all twins.
=#
mutable struct Twin
    router::AbstractRouter
    id::String
    session::Dict{String,Any}
    hasname::Bool
    isauth::Bool
    sock::Any
    retroactive::Dict{String,Bool}
    out::Channel # router inbox
    sent::Dict{UInt128,Any} # msg.id => timestamp of sending
    acktimer::Dict{UInt128,Timer}
    qos::QoS
    pager::Union{Nothing,Pager}
    reactive::Bool
    process::Visor.Process

    Twin(router, id, out_channel) = new(
        router,
        id,
        Dict(),
        false,
        false,
        nothing,
        Dict(),
        out_channel,
        Dict(),
        Dict(),
        fast,
        nothing,
        false,
    )
end

mutable struct Msg
    ptype::UInt8
    content::RembusMsg
    twchannel::Twin
    reqdata::Any
    Msg(ptype, content, src) = new(ptype, content, src)
    Msg(ptype, content, src, reqdata) = new(ptype, content, src, reqdata)
end

struct SentData
    sending_ts::Float64
    request::Msg
end

mutable struct Embedded <: AbstractRouter
    topic_function::Dict{String,Function}
    id_twin::Dict{String,Twin}
    process::Visor.Process
    ws_server::Sockets.TCPServer
    Embedded() = new(Dict(), Dict())
end

"""
    embedded()

Initialize an embedded server for brokerless rpc and one way pub/sub.
"""
embedded() = Embedded()

mutable struct Router <: AbstractRouter
    start_ts::Float64
    address2twin::Dict{Vector{UInt8},Twin} # zeromq address => twin
    twin2address::Dict{String,Vector{UInt8}} # twin id => zeromq address
    mid2address::Dict{UInt128,Vector{UInt8}} # message.id => zeromq connection address
    topic_impls::Dict{String,OrderedSet{Twin}} # topic => twins implementor
    last_invoked::Dict{String,Int} # topic => twin index last called
    topic_interests::Dict{String,Set{Twin}} # topic => twins subscribed to topic
    id_twin::Dict{String,Twin}
    topic_function::Dict{String,Function}
    topic_auth::Dict{String,Dict{String,Bool}} # topic => {twin.id => true}
    admins::Set{String}
    twin_initialize::Function
    twin_finalize::Function
    park::Function
    unpark::Function
    process::Visor.Process
    server::Sockets.TCPServer
    ws_server::Sockets.TCPServer
    zmqsocket::ZMQ.Socket
    owners::DataFrame
    component_owner::DataFrame
    Router() = new(
        time(),
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
        park,
        unpark
    )
end

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

macro mlog(str)
    quote
        if CONFIG.metering
            @info $(esc(str))
        end
    end
end

macro rawlog(msg)
    quote
        if CONFIG.rawdump
            @info $(esc(msg))
        end
    end
end

## Twin related functions

twin_initialize(ctx, twin) = (ctx, twin) -> ()

twin_finalize(ctx, twin) = (ctx, twin) -> ()

offline(twin::Twin) = ((twin.sock === nothing) || !isopen(twin.sock))

session(twin::Twin) = twin.session

function create_twin(id, router::Embedded, queue=Queue{PubSubMsg}())
    if haskey(router.id_twin, id)
        return router.id_twin[id]
    else
        twin = Twin(router, id, Channel())
        spec = process(id, twin_task, args=(twin,))
        startup(from("caronte.twins"), spec)
        router.id_twin[id] = twin
        return twin
    end
end

named_twin(id, router) = haskey(router.id_twin, id) ? router.id_twin[id] : nothing

function create_twin(id, router)
    if haskey(router.id_twin, id)
        return router.id_twin[id]
    else
        twin = Twin(router, id, router.process.inbox)
        spec = process(id, twin_task, args=(twin,))
        startup(from("caronte.twins"), spec)
        router.id_twin[id] = twin
        twin_initialize(CONFIG.broker_ctx, twin)
        return twin
    end
end

#=
    offline!(twin)

Unbind the ZMQ socket from the twin.
=#
function offline!(twin)
    @debug "[$twin] closing: going offline"
    twin.sock = nothing

    return nothing
end

#=
    destroy_twin(twin, router)

Remove the twin from the system.

Shutdown the process and remove the twin from the router.
=#
function destroy_twin(twin, router)
    if isdefined(twin, :process)
        Visor.shutdown(twin.process)
    end

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

function destroy_twin(twin, router::Embedded)
    if isdefined(twin, :process)
        Visor.shutdown(twin.process)
    end

    delete!(router.id_twin, twin.id)
    return nothing
end

function verify_signature(twin, msg)
    challenge = pop!(twin.session, "challenge")
    @debug "verify signature, challenge $challenge"
    file = pubkey_file(msg.cid)

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
    namedtwin = create_twin(msg.cid, router)
    # move the opened socket
    namedtwin.sock = twin.sock
    #create a pager
    if paging
        namedtwin.pager = Pager(namedtwin)
    end

    # destroy the anonymous process
    destroy_twin(twin, router)
    namedtwin.hasname = true
    namedtwin.isauth = isauth
    return namedtwin
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
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end

    response = ResMsg(msg.id, sts, reason)
    @mlog("[$twin] -> $response")
    transport_send(twin, twin.sock, response)

    if sts !== STS_SUCCESS
        detach(twin)
    end

    return authtwin
end

function attestation(router::Embedded, twin, msg)
    @debug "[$twin] authenticating cid: $(msg.cid)"
    sts = STS_SUCCESS
    reason = nothing
    try
        login(router, twin, msg)
        @debug "[$(msg.cid)] is authenticated"
        twin.hasname = true
        twin.isauth = true
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end

    response = ResMsg(msg.id, sts, reason)
    @mlog("[$twin] -> $response")
    transport_send(twin, twin.sock, response)

    if sts !== STS_SUCCESS
        detach(twin)
    end

    return twin
end

function get_token(router, userid, id::UInt128)
    vals = UInt8[(id>>24)&0xff, (id>>16)&0xff, (id>>8)&0xff, id&0xff]
    token = bytes2hex(vals)
    df = router.owners[(router.owners.pin.==token).&(router.owners.uid.==userid), :]
    if isempty(df)
        @info "user [$userid]: invalid token"
        return nothing
    else
        @debug "user [$userid]: token is valid"
        return token
    end
end

#=
    register(router, twin, msg)

Register a component.
=#
function register(router, twin, msg)
    @debug "[$(twin.id)] registering pubkey of $(msg.cid), id: $(msg.id)"
    sts = STS_SUCCESS
    reason = nothing
    token = get_token(router, msg.userid, msg.id)
    if token === nothing
        sts = STS_GENERIC_ERROR
        reason = "wrong pin"
    elseif isregistered(msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        save_pubkey(msg.cid, msg.pubkey)
        if !(msg.cid in router.component_owner.component)
            push!(router.component_owner, [msg.userid, msg.cid])
        end
        save_token_app(router.component_owner)
    end
    response = ResMsg(msg.id, sts, reason)
    @mlog("[$twin] -> $response")
    transport_send(twin, twin.sock, response)
end

#=
    unregister(twin, msg)

Unregister a component.
=#
function unregister(router, twin, msg)
    @debug "[$twin] unregistering $(msg.cid), isauth: $(twin.isauth)"
    sts = STS_SUCCESS
    reason = nothing

    if !twin.isauth
        sts = STS_GENERIC_ERROR
        reason = "invalid operation"
    elseif twin.id != msg.cid
        sts = STS_GENERIC_ERROR
        reason = "invalid cid"
    else
        remove_pubkey(msg.cid)
        deleteat!(router.component_owner, router.component_owner.component .== msg.cid)
        save_token_app(router.component_owner)
    end
    response = ResMsg(msg.id, sts, reason)
    @mlog("[$twin] -> $response")
    transport_send(twin, twin.sock, response)
end

function rpc_response(router, twin, msg)
    if haskey(twin.sent, msg.id)
        twinput = twin.sent[msg.id].request.twchannel
        reqdata = twin.sent[msg.id].request.content
        put!(router.process.inbox, Msg(TYPE_RESPONSE, msg, twinput, reqdata))

        elapsed = time() - twin.sent[msg.id].sending_ts
        if CONFIG.metering
            @debug "[$(twin.id)][$(reqdata.topic)] exec elapsed time: $elapsed secs"
        end

        delete!(twin.sent, msg.id)
    else
        @error "[$twin] unexpected response: $msg"
    end
end

function admin_msg(router, twin, msg)
    admin_res = admin_command(router, twin, msg)
    @debug "admin response: $admin_res"
    push!(twin.process.inbox, admin_res)
    return nothing
end

function embedded_msg(router::Embedded, twin::Twin, msg::RembusMsg)
    (found, resmsg) = embedded_eval(router, twin, msg)

    if found
        if isa(resmsg, ResMsg)
            response = Msg(TYPE_RESPONSE, resmsg, twin)
            respond(router, response)
        end
    else
        @debug "[embedded] no provider for [$(msg.topic)]"
        if isa(msg, RpcReqMsg)
            response = Msg(TYPE_RESPONSE, ResMsg(msg, STS_METHOD_NOT_FOUND, nothing), twin)
            respond(router, response)
        end
    end

    return nothing
end

rpc_request(router::Embedded, twin, msg) = embedded_msg(router, twin, msg)

pubsub_msg(router::Embedded, twin, msg) = embedded_msg(router, twin, msg)

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

function pubsub_msg(router, twin, msg)
    if !isauthorized(router, twin, msg.topic)
        @warn "[$twin] is not authorized to publish on $(msg.topic)"
    else
        # msg is routable, get it to router
        @debug "[$twin] to router: $(prettystr(msg))"
        put!(router.process.inbox, Msg(TYPE_PUB, msg, twin))
    end

    return nothing
end

function ack_msg(twin, msg)
    if twin.qos === with_ack
        msgid = msg.hash
        if haskey(twin.acktimer, msgid)
            close(twin.acktimer[msgid])
            delete!(twin.acktimer, msgid)
        end
    end

    return nothing
end

function receiver_exception(router, twin, e)
    if isconnectionerror(twin.sock, e)
        if close_is_ok(twin.sock, e)
            @debug "[$twin] connection closed"
        else
            @error "[$twin] connection closed: $e"
        end
    elseif isa(e, InterruptException)
        rethrow()
    elseif isa(e, ArgumentError)
        @error "[$twin] invalid message format: $e"
    else
        @error "[$twin] internal error: $e"
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

#=
    twin_receiver(router, twin)

Receive messages from the client socket.
=#
function twin_receiver(router, twin)
    @debug "client [$(twin.id)] is connected"
    try
        ws = twin.sock
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.sock = nothing
                @debug "client [$(twin.id)]: connection close"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            @mlog("[$(twin.id)] <- $(prettystr(msg))")

            if isa(msg, Unregister)
                unregister(router, twin, msg)
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
            else
                error("unexpected rembus message")
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

function isauthenticated(session)
    return haskey(session, "isauthenticated") && session["isauthenticated"] == true
end

function challenge(router, twin, msg)
    if isauthenticated(twin.session)
        error("already authenticated")
    elseif haskey(router.topic_function, "challenge")
        challenge = router.topic_function["challenge"](twin)
    else
        challenge = rand(RandomDevice(), UInt8, 4)
    end
    twin.session["challenge"] = challenge
    return ResMsg(msg.id, STS_CHALLENGE, challenge)
end

#=
    anonymous_twin_receiver(router, twin)

Receive messages from the client socket.
=#
function anonymous_twin_receiver(router, twin)
    @debug "anonymous client [$(twin.id)] is connected"
    try
        ws = twin.sock
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.sock = nothing
                @debug "client [$(twin.id)]: connection close"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            @mlog("[$(twin.id)] <- $(prettystr(msg))")
            if isa(msg, IdentityMsg)
                auth_twin = identity_check(router, twin, msg, paging=true)
                if auth_twin !== nothing
                    return auth_twin
                end
            elseif isa(msg, Register)
                register(router, twin, msg)
            elseif isa(msg, Attestation)
                return attestation(router, twin, msg)
            elseif isa(msg, ResMsg)
                rpc_response(router, twin, msg)
            elseif isa(msg, AdminReqMsg)
                admin_msg(router, twin, msg)
            elseif isa(msg, RpcReqMsg)
                rpc_request(router, twin, msg)
            elseif isa(msg, PubSubMsg)
                pubsub_msg(router, twin, msg)
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

function zeromq_receiver(router::Router)
    while true
        try
            pkt::ZMQPacket = zmq_message(router)
            id = pkt.identity

            if haskey(router.address2twin, id)
                twin = router.address2twin[id]
            else
                @debug "creating anonymous twin from identity $id ($(bytes2zid(id)))"
                # create the twin
                twin = create_twin(string(bytes2zid(id)), router)
                @debug "[anonymous] client bound to twin id [$twin]"
                router.address2twin[id] = twin
                router.twin2address[twin.id] = id
                twin.sock = router.zmqsocket
            end

            msg::RembusMsg = broker_parse(router, pkt)
            @mlog("[ZMQ][$twin] <- $(prettystr(msg))")

            if isa(msg, IdentityMsg)
                @debug "[$twin] auth identity: $(msg.cid)"
                # check if cid is registered
                rembus_login = isfile(key_file(msg.cid))
                if rembus_login
                    # authentication mode, send the challenge
                    response = challenge(router, twin, msg)
                else
                    identity_upgrade(router, twin, msg, id, authenticate=false)
                    continue
                end
                @mlog("[ZMQ][$twin] -> $response")
                transport_send(twin, router.zmqsocket, response)
            elseif isa(msg, PingMsg)
                if (twin.id != msg.cid)
                    # broker restarted
                    # start the authentication flow if cid is registered
                    @debug "lost connection to broker: restarting $(msg.cid)"
                    rembus_login = isfile(key_file(msg.cid))
                    if rembus_login
                        response = challenge(router, twin, msg)
                        transport_send(twin, router.zmqsocket, response)
                    else
                        identity_upgrade(router, twin, msg, id, authenticate=false)
                    end

                else
                    if twin.sock !== nothing
                        pong(twin.sock, msg.id, id)

                        # check if there are cached messages
                        if twin.reactive
                            unpark(CONFIG.broker_ctx, twin)
                        end
                    end
                end
            elseif isa(msg, Register)
                register(router, twin, msg)
            elseif isa(msg, Unregister)
                unregister(router, twin, msg)
            elseif isa(msg, Attestation)
                identity_upgrade(router, twin, msg, id, authenticate=true)
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
            elseif isa(msg, Close)
                offline!(twin)
            elseif isa(msg, Remove)
                destroy_twin(twin, router)
            end
        catch e
            if isa(e, Visor.ProcessInterrupt) || isa(e, ZMQ.StateError)
                rethrow()
            end
            @warn "[ZMQ] recv error: $e"
            @showerror e
        end
    end
end

function identity_upgrade(router, twin, msg, id; authenticate=false)
    newtwin = attestation(router, twin, msg, authenticate)
    if newtwin !== nothing
        router.address2twin[id] = newtwin
        delete!(router.twin2address, twin.id)
        router.twin2address[newtwin.id] = id
    end

    return nothing
end

function close_is_ok(ws::WebSockets.WebSocket, e)
    HTTP.WebSockets.isok(e)
end

function close_is_ok(ws::TCPSocket, e)
    isa(e, ConnectionClosed)
end

close_is_ok(::Nothing, e) = true

page_file(twin) = joinpath(twins_dir(), twin.id, string(twin.pager.ts))

#=
    detach(twin)

Disconnect the twin from the ws/tcp channel.
=#
function detach(twin)
    if twin.sock !== nothing
        try
            if !isa(twin.sock, ZMQ.Socket)
                close(twin.sock)
            end
        catch e
            @debug "error closing websocket: $e"
        end
        twin.sock = nothing
    end

    return nothing
end

#=
    twin_task(self, twin)

Twin task that read messages from router and send to client.

It enqueues the input messages if the component is offline.
=#
function twin_task(self, twin)
    twin.process = self
    try
        @debug "starting twin [$(twin.id)]"
        for msg in self.inbox
            if isshutdown(msg)
                break
            elseif isa(msg, ResMsg)
                @mlog("[$(twin.id)] -> $msg")
                transport_send(twin, twin.sock, msg, true)
            elseif isa(msg, EnableReactiveMsg)
                @mlog("[$(twin.id)] -> $msg")
                transport_send(twin, twin.sock, ResMsg(msg.id, STS_SUCCESS, nothing), true)
                twin.router.unpark(CONFIG.broker_ctx, twin)
            else
                signal!(twin, msg)
            end
        end
    catch e
        @error "twin_task: $e" exception = (e, catch_backtrace())
        rethrow()
    end
    @debug "[$twin] task done"
end

acklock = ReentrantLock()

#=
    handle_ack_timeout(tim, twin, msg, msgid)

Persist a PubSub message in case the acknowledge message is not received.
=#
function handle_ack_timeout(tim, twin, msg, msgid)
    lock(acklock) do
        if haskey(twin.acktimer, msgid)
            try
                twin.router.park(CONFIG.broker_ctx, twin, msg)
            catch e
                @error "[$twin] ack_timeout: $e"
            end
        end
        delete!(twin.acktimer, msgid)
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
function signal!(twin, msg)
    @debug "[$twin] message>>: $msg, offline:$(offline(twin)), type:$(msg.ptype)"
    if (offline(twin) || notreactive(twin, msg)) && msg.ptype === TYPE_PUB
        twin.router.park(CONFIG.broker_ctx, twin, msg.content)
        return nothing

    elseif twin.pager !== nothing && twin.pager.io !== nothing
        # it is online and reactive, send cached messages if any
        twin.router.unpark(CONFIG.broker_ctx, twin)
    end

    # current message
    if isa(msg.content, RpcReqMsg)
        # add to sent table
        twin.sent[msg.content.id] = SentData(time(), msg)
    end

    pkt = msg.content
    @mlog "[$twin] -> $pkt"
    try
        transport_send(twin, twin.sock, pkt)
    catch e
        @debug "[$twin] going offline: $e"
        @showerror e
        if msg.ptype === TYPE_PUB
            twin.router.park(CONFIG.broker_ctx, twin, msg.content)
        end
        detach(twin)
    end

    return nothing
end

### # Return true if Twin is interested to the message.
### function interested(twin, message::Dict)::Bool
###     true
### end

#=
    callback_or(fn::Function, router::AbstractRouter, callback::Symbol)

Invoke `callback` function if it is injected via the plugin module otherwise invoke `fn`.
=#
function callback_or(fn::Function, router::AbstractRouter, callback::Symbol)
    if CONFIG.broker_plugin !== nothing && isdefined(CONFIG.broker_plugin, callback)
        cb = getfield(CONFIG.broker_plugin, callback)
        cb(CONFIG.broker_ctx, router)
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
        if CONFIG.broker_plugin !== nothing && isdefined(CONFIG.broker_plugin, cb)
            cb = getfield(CONFIG.broker_plugin, cb)
            cb(CONFIG.broker_ctx, router, twin, msg)
        end
        fn()
    catch e
        @error "$cb callback error: $e"
    end
end

#=
    set_broker_plugin(extension::Module)

Inject the module that implements the functions invoked at specific entry points of
twin state machine.
=#
function set_broker_plugin(extension::Module)
    CONFIG.broker_plugin = extension
end

#=
    set_broker_context(ctx)

Set the object to be passed ad first argument to functions related to twin lifecycle.

Actually the functions that use `ctx` are:

- `twin_initialize`
- `twin_finalize`
- `park`
- `unpark`
=#
function set_broker_context(ctx)
    CONFIG.broker_ctx = ctx
end

function command_line()
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--reset", "-r"
        help = "factory reset, clean up broker configuration"
        action = :store_true
        "--secure", "-s"
        help = "accept wss and tls connections on BROKER_WS_PORT and BROKER_TCP_PORT"
        action = :store_true
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
    end
    return parse_args(s)
end

function caronte_reset()
    rm(twins_dir(), force=true, recursive=true)
    if isdir(root_dir())
        foreach(rm, filter(isfile, readdir(root_dir(), join=true)))
    end
end

"""
    caronte(; wait=true, exit_when_done=true)

Start the broker.

Return immediately when `wait` is false, otherwise blocks until shut down.

Return instead of exiting if `exit_when_done` is false.
"""
function caronte(; wait=true, exit_when_done=true, args=Dict())
    if isempty(args)
        args = command_line()
    end

    if haskey(args, "debug") && args["debug"] === true
        ENV["REMBUS_DEBUG"] = "1"
    end

    if haskey(args, "reset") && args["reset"] === true
        Rembus.caronte_reset()
    end

    issecure = get(args, "secure", false)

    setup(CONFIG)
    router = Router()

    tasks = [
        supervisor("twins", terminateif=:shutdown),
        process(broker, args=(router,)),
        process(serve_tcp, args=(router, issecure), restart=:transient),
        process(serve_ws, args=(router, issecure), restart=:transient, stop_waiting_after=2.0),
        process(serve_zeromq, args=(router,), restart=:transient, debounce_time=2)
    ]
    sv = supervise(
        [supervisor("caronte", tasks, strategy=:one_for_all, intensity=0)],
        wait=wait
    )
    if exit_when_done
        exit(0)
    end

    return sv
end

function caronted()::Cint
    caronte()
    return 0
end

"""
    serve(embedded::Embedded; wait=true, exit_when_done=true, secure=false)

Start an embedded server and accept connections.
"""
function serve(embedded::Embedded; wait=true, exit_when_done=true, secure=false)
    setup(CONFIG)
    init_log()

    tasks = [
        supervisor("twins", terminateif=:shutdown),
        process(
            serve_ws,
            args=(embedded, secure),
            restart=:transient,
            stop_waiting_after=2.0
        ),
    ]
    sv = supervise(
        [supervisor("caronte", tasks, strategy=:one_for_one)],
        intensity=5,
        wait=wait
    )
    if exit_when_done
        exit(0)
    end

    return sv
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

function client_receiver(router::Router, sock)
    cid = string(uuid4())
    twin = create_twin(cid, router)
    @debug "[anonymous] client bound to twin id [$cid]"

    # start the trusted client task
    twin.sock = sock

    # ws/tcp socket receiver task
    authtwin = anonymous_twin_receiver(router, twin)

    # upgrade to named or authenticated twin if it returns true
    if (authtwin !== nothing)
        twin_receiver(router, authtwin)
    end

    return nothing
end

function identity_check(router, twin, msg; paging=true)
    @debug "[$twin] auth identity: $(msg.cid)"
    if isempty(msg.cid)
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "empty cid", twin))
    end
    named = named_twin(msg.cid, router)
    if named !== nothing && !offline(named)
        @warn "a component with id [$(msg.cid)] is already connected"
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "already connected"), twin)
    else
        # check if cid is registered
        rembus_login = isfile(key_file(msg.cid))
        if rembus_login
            # authentication mode, send the challenge
            response = challenge(router, twin, msg)
            respond(router, response, twin)
        else
            authtwin = setidentity(router, twin, msg, paging=paging)
            respond(router, ResMsg(msg.id, STS_SUCCESS, nothing), authtwin)
            return authtwin
        end
    end
    return nothing
end

function client_receiver(router::Embedded, ws)
    cid = string(uuid4())
    twin = create_twin(cid, router)
    @debug "[embedded] client bound to twin id [$cid]"
    # start the trusted client task
    twin.sock = ws
    while isopen(ws)
        try
            payload = transport_read(ws)
            if isempty(payload)
                @debug "[$twin]: connection close"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            @mlog("[$twin] <- $(prettystr(msg))")

            if isa(msg, RpcReqMsg)
                rpc_request(router, twin, msg)
            elseif isa(msg, PubSubMsg)
                pubsub_msg(router, twin, msg)
            elseif isa(msg, IdentityMsg)
                identity_check(router, twin, msg, paging=false)
            elseif isa(msg, Attestation)
                attestation(router, twin, msg)
            elseif isa(msg, AckMsg)
                ack_msg(twin, msg)
            end
        catch e
            @debug "[embedded] error: $e"
            if isa(e, EOFError)
                break
            end
        end
    end
    end_receiver(twin)

    return nothing
end

function secure_config()
    trust_store = keystore_dir()

    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)

    sslconfig = MbedTLS.SSLConfig(
        joinpath(trust_store, "caronte.crt"),
        joinpath(trust_store, "caronte.key")
    )
    MbedTLS.rng!(sslconfig, rng)

    function show_debug(level, filename, number, msg)
        @show level, filename, number, msg
    end

    MbedTLS.dbg!(sslconfig, show_debug)
    return sslconfig
end

function listener(proc, router, sslconfig)
    IP = "0.0.0.0"
    caronte_port = parse(UInt16, get(ENV, "BROKER_WS_PORT", "8000"))

    server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
    router.ws_server = server
    proto = (sslconfig === nothing) ? "ws" : "wss"
    @info "caronte up and running at port $proto:$caronte_port"

    setphase(proc, :listen)

    HTTP.WebSockets.listen(IP, caronte_port, server=server, sslconfig=sslconfig) do ws
        client_receiver(router, ws)
    end
end

function serve_ws(td, router, issecure=false)
    sslconfig = nothing

    try
        if issecure
            sslconfig = secure_config()
        end

        listener(td, router, sslconfig)
    catch e
        if !isa(e, Visor.ProcessInterrupt)
            @error "ws server: $e"
            @showerror e
        end
        rethrow()
    finally
        @debug "serve_ws closed"
        setphase(td, :terminate)
        isdefined(router, :ws_server) && close(router.ws_server)
    end
end

function serve_zeromq(pd, router)
    @debug "starting serve_zeromq [$(pd.id)]"
    port = parse(UInt16, get(ENV, "BROKER_ZMQ_PORT", "8002"))
    context = ZMQ.Context()
    router.zmqsocket = Socket(context, ROUTER)
    ZMQ.bind(router.zmqsocket, "tcp://*:$port")

    try
        @info "caronte up and running at port zmq:$port"
        setphase(pd, :listen)
        zeromq_receiver(router)
    catch e
        # consider ProcessInterrupt a normal termination because
        # zeromq_receiver is not polling for supervisor shutdown message
        if !isa(e, Visor.ProcessInterrupt)
            @error "[serve_zeromq] error: $e"
            rethrow()
        end
    finally
        setphase(pd, :terminate)
        ZMQ.close(router.zmqsocket)
        ZMQ.close(context)
    end
end

function serve_tcp(pd, router, issecure=false)
    proto = "tcp"
    server = nothing
    try
        IP = "0.0.0.0"
        caronte_port = parse(UInt16, get(ENV, "BROKER_TCP_PORT", "8001"))
        setphase(pd, :listen)

        if issecure
            proto = "tls"
            sslconfig = secure_config()

            server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
            router.server = server
            @info "caronte up and running at port $proto:$caronte_port"
            while true
                try
                    sock = accept(server)
                    ctx = MbedTLS.SSLContext()
                    MbedTLS.setup!(ctx, sslconfig)
                    MbedTLS.associate!(ctx, sock)
                    MbedTLS.handshake(ctx)
                    @async client_receiver(router, ctx)
                catch e
                    if !isa(e, Visor.ProcessInterrupt)
                        @error "tcp server: $e"
                        @showerror e
                    end
                    rethrow()
                end
            end
        else
            server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
            router.server = server
            @info "caronte up and running at port $proto:$caronte_port"
            while true
                try
                    sock = accept(server)
                    @async client_receiver(router, sock)
                catch e
                    if !isa(e, Visor.ProcessInterrupt)
                        @error "tcp server: $e"
                        @showerror e
                    end
                    rethrow()
                end
            end
        end
    finally
        setphase(pd, :terminate)
        server !== nothing && close(server)
    end
end

function islistening(wait=5)
    procs = ["caronte.serve_ws", "caronte.serve_tcp", "caronte.serve_zeromq"]
    tcount = 0
    while tcount < wait
        if all(p -> getphase(p) === :listen, from.(procs))
            return true
        end
        tcount += 0.2
    end

    return false
end

isconnected(twin) = twin.sock !== nothing && isopen(twin.sock)

function first_up(router, topic, implementors)
    @debug "[$topic] first_up balancer"
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
            for impl in implementors
                current_index += 1
                !isconnected(impl) && continue
                target = impl
                router.last_invoked[topic] = current_index
                break
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
    @info "[$topic] balancer: $(CONFIG.balancer)"
    if CONFIG.balancer === "first_up"
        target = first_up(router, topic, implementors)
    elseif CONFIG.balancer === "round_robin"
        target = round_robin(router, topic, implementors)
    elseif CONFIG.balancer === "less_busy"
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
    else
        @debug "no broadcast for [$msg]: request data not available or embedded method"
        return nothing
    end

    union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))
    newmsg = Msg(TYPE_PUB, bmsg, msg.twchannel)

    for tw in filter(t -> t.process.inbox != msg.twchannel.process.inbox, authtwins)
        @debug "broadcasting $topic to $(tw.id): [$newmsg]"
        put!(tw.process.inbox, newmsg)
    end

    return nothing
end

"""
    isauthorized(session)

Return true if the connected component is authenticated.
"""
isauthorized(session) = session.isauth

#=
    isauthorized(router::Router, twin::Twin, topic::AbstractString)

Return true if topic is public or client is authorized to bind to topic.
=#
function isauthorized(router::Router, twin::Twin, topic::AbstractString)
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

respond(::Embedded, msg::Msg) = put!(msg.twchannel.process.inbox, msg)

respond(::AbstractRouter, msg::RembusMsg, twin::Twin) = put!(twin.process.inbox, msg)

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
            payload = decode(msg.data)
        end
        try
            result = router.topic_function[msg.topic](twin, getargs(payload)...)
            sts = STS_SUCCESS
        catch e
            @debug "[$(msg.topic)] embedded error (method too young?): $e"
            result = "$e"
            sts = STS_METHOD_EXCEPTION

            if isa(e, MethodError)
                try
                    result = Base.invokelatest(
                        router.topic_function[msg.topic],
                        twin,
                        getargs(payload)...
                    )
                    sts = STS_SUCCESS
                catch e
                    result = "$e"
                end
            end
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

#=
    broker(self, router)

Rembus broker main task.
=#
function broker(self, router)
    @debug "[broker] starting"
    try
        router.process = self
        init(router)

        # example for registering a broker implementor
        router.topic_function["uptime"] = (session) -> uptime(router)
        router.topic_function["version"] = (session) -> Rembus.VERSION

        for msg in self.inbox
            # process control messages
            !isshutdown(msg) || break

            @debug "[broker] recv $(typeof(msg)): $msg"
            if isa(msg, Msg)
                if msg.ptype == TYPE_PUB
                    if !caronte_embedded_method(router, msg.twchannel, msg.content)
                        # publish to interested twins
                        broadcast!(router, msg)
                    end
                elseif msg.ptype == TYPE_RPC
                    topic = msg.content.topic
                    if caronte_embedded_method(router, msg.twchannel, msg.content)
                    else
                        # find an implementor
                        if haskey(router.topic_impls, topic)
                            # request a method exec
                            @debug "[broker] finding an target implementor for $topic"
                            implementors = router.topic_impls[topic]
                            target = select_twin(router, topic, implementors)
                            @debug "[broker] target implementor: [$target]"
                            if target === nothing
                                msg.content = ResMsg(msg.content, STS_METHOD_UNAVAILABLE)
                                put!(msg.twchannel.process.inbox, msg)
                            elseif target.process.inbox === msg.twchannel.process.inbox
                                @warn "[$(target.id)]: loopback detected"
                                msg.content = ResMsg(msg.content, STS_METHOD_LOOPBACK)
                                put!(msg.twchannel.process.inbox, msg)
                            elseif target !== nothing
                                @debug "implementor target: $(target.id)"
                                put!(target.process.inbox, msg)
                            end

                        else
                            # method implementor not found
                            msg.content = ResMsg(msg.content, STS_METHOD_NOT_FOUND)
                            put!(msg.twchannel.process.inbox, msg)
                        end
                    end
                elseif msg.ptype == TYPE_RESPONSE
                    # it is a result from an implementor
                    # reply toward the client that has made the request
                    respond(router, msg)
                end
                # @debug "waiting next msg ..."
            else
                @warn "unknow $(typeof(msg)) message $msg "
            end
        end
    catch e
        @error "[broker] error: $e"
        @showerror e
        rethrow()
    finally
        for tw in values(router.id_twin)
            ## detach(tw)
            save_page(tw)
        end
        save_configuration(router)
    end
    @debug "[broker] done"
end

function caronte_context(fn, ctx)
    if ctx === nothing
        return data -> fn(data...)
    else
        return data -> fn(ctx, data...)
    end
end

function eval_optional(router, modname, fname)
    sts = eval(Meta.parse("isdefined(Main.$modname, :$fname)"))
    if sts
        return Base.invokelatest(Base.eval(Main, Meta.parse("$modname.$fname")), router)
    else
        return nothing
    end
end

#=
    boot(router)

Setup the router.
=#
function boot(router)
    if !isdir(CONFIG.db)
        mkdir(CONFIG.db)
    end

    appdir = keys_dir()
    if !isdir(appdir)
        mkdir(appdir)
    end

    twin_dir = twins_dir()
    if !isdir(twin_dir)
        mkdir(twin_dir)
    end

    load_configuration(router)
    return nothing
end

function init_log()
    if isinteractive()
        Logging.disable_logging(Logging.Info)
    else
        logging(debug=[])
    end
end

function init(router)
    init_log()
    boot(router)
    @debug "broker datadir: $(CONFIG.db)"

    if CONFIG.broker_plugin !== nothing
        if isdefined(CONFIG.broker_plugin, :park) &&
           isdefined(CONFIG.broker_plugin, :unpark)
            router.park = getfield(CONFIG.broker_plugin, :park)
            router.unpark = getfield(CONFIG.broker_plugin, :unpark)
        end

        if isdefined(CONFIG.broker_plugin, :twin_initialize)
            router.twin_initialize = getfield(CONFIG.broker_plugin, :twin_initialize)
        end
        if isdefined(CONFIG.broker_plugin, :twin_finalize)
            router.twin_finalize = getfield(CONFIG.broker_plugin, :twin_finalize)
        end

        topics = names(CONFIG.broker_plugin)
        exposed = filter(
            sym -> isa(sym, Function),
            [getfield(Rembus.CONFIG.broker_plugin, t) for t in topics]
        )
        for topic in exposed
            router.topic_function[string(topic)] = topic
        end
    end

    return nothing
end
