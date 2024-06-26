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

`socket` is the socket handle when the protocol is tcp/tls or ws/wss.

For ZMQ sockets one socket is shared between all twins.
=#
mutable struct Twin
    router::AbstractRouter
    id::String
    session::Dict{String,Any}
    hasname::Bool
    isauth::Bool
    socket::Any
    retroactive::Dict{String,Bool}
    sent::Dict{UInt128,Any} # msg.id => timestamp of sending
    out::Dict{UInt128,Threads.Condition}
    acktimer::Dict{UInt128,Timer}
    qos::QoS
    pager::Union{Nothing,Pager}
    reactive::Bool
    ack_cond::Dict{UInt128,Threads.Condition}
    process::Visor.Process

    Twin(router, id) = new(
        router,
        id,
        Dict(),
        false,
        false,
        nothing,
        Dict(),
        Dict(),
        Dict(),
        Dict(),
        fast,
        nothing,
        false,
        Dict()
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
    context::Any
    process::Visor.Supervisor
    ws_server::Sockets.TCPServer
    owners::DataFrame
    component_owner::DataFrame
    Embedded(context=nothing) = new(Dict(), Dict(), context)
end

"""
    server(ctx=nothing)

Initialize a server for brokerless rpc and one way pub/sub.
"""
server(ctx=nothing) = Embedded(ctx)

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
    pub_handler::Union{Nothing,Function}
    park::Function
    unpark::Function
    plugin::Union{Nothing,Module}
    context::Any
    process::Visor.Process
    server::Sockets.TCPServer
    http_server::HTTP.Server
    ws_server::Sockets.TCPServer
    zmqsocket::ZMQ.Socket
    owners::DataFrame
    component_owner::DataFrame
    Router(plugin=nothing, context=nothing) = new(
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
        nothing,
        park,
        unpark,
        plugin, # plugin
        context  # context
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

## Twin related functions

twin_initialize(ctx, twin) = (ctx, twin) -> ()

twin_finalize(ctx, twin) = (ctx, twin) -> ()

Base.isopen(c::Condition) = true

offline(twin::Twin) = ((twin.socket === nothing) || !isopen(twin.socket))

session(twin::Twin) = twin.session

function create_twin(id, router::Embedded, queue=Queue{PubSubMsg}())
    if haskey(router.id_twin, id)
        return router.id_twin[id]
    else
        twin = Twin(router, id)
        spec = process(id, twin_task, args=(twin,))
        sv = router.process
        startup(Visor.from_supervisor(sv, "twins"), spec)
        router.id_twin[id] = twin
        return twin
    end
end

named_twin(id, router) = haskey(router.id_twin, id) ? router.id_twin[id] : nothing

function create_twin(id, router)
    if haskey(router.id_twin, id)
        return router.id_twin[id]
    else
        twin = Twin(router, id)
        spec = process(id, twin_task, args=(twin,))
        sv = router.process.supervisor
        startup(Visor.from_supervisor(sv, "twins"), spec)
        yield()
        router.id_twin[id] = twin
        twin_initialize(router.context, twin)
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

    return nothing
end

function cleanup(twin, router::Router)
    # save the cache
    save_page(twin)

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

function cleanup(twin, router::Embedded)
    delete!(router.id_twin, twin.id)
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
    return cleanup(twin, router)
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
    namedtwin = create_twin(msg.cid, router)
    # move the opened socket
    namedtwin.socket = twin.socket
    if !isa(twin.socket, ZMQ.Socket)
        twin.socket = nothing
    end

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
        transport_send(authtwin, authtwin.socket, ResMsg(msg.id, sts, reason))
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
        transport_send(twin, twin.socket, ResMsg(msg.id, sts, reason))
    end

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
        twin.id = msg.cid
        twin.hasname = true
        twin.isauth = true
    catch e
        @error "[$(msg.cid)] attestation: $e"
        sts = STS_GENERIC_ERROR
        reason = isa(e, ErrorException) ? e.msg : string(e)
    end

    response = ResMsg(msg.id, sts, reason)
    #@mlog("[$twin] -> $response")
    transport_send(twin, twin.socket, response)
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
        reason = "wrong uid/pin"
    elseif isregistered(router, msg.cid)
        sts = STS_NAME_ALREADY_TAKEN
        reason = "name $(msg.cid) not available for registration"
    else
        kdir = keys_dir(router)
        if !isdir(kdir)
            mkdir(kdir)
        end

        save_pubkey(router, msg.cid, msg.pubkey)
        if !(msg.cid in router.component_owner.component)
            push!(router.component_owner, [msg.userid, msg.cid])
        end
        save_token_app(router, router.component_owner)
    end
    response = ResMsg(msg.id, sts, reason)
    #@mlog("[$twin] -> $response")
    put!(twin.process.inbox, response)
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
        remove_pubkey(router, msg.cid)
        deleteat!(router.component_owner, router.component_owner.component .== msg.cid)
        save_token_app(router, router.component_owner)
    end
    response = ResMsg(msg.id, sts, reason)
    #@mlog("[$twin] -> $response")
    put!(twin.process.inbox, response)
end

function rpc_response(router, twin, msg)
    if haskey(twin.out, msg.id)
        lock(twin.out[msg.id]) do
            notify(twin.out[msg.id], msg)
        end
    end

    if haskey(twin.sent, msg.id)
        twinput = twin.sent[msg.id].request.twchannel
        reqdata = twin.sent[msg.id].request.content
        put!(router.process.inbox, Msg(TYPE_RESPONSE, msg, twinput, reqdata))

        elapsed = time() - twin.sent[msg.id].sending_ts
        #if CONFIG.metering
        #    @debug "[$(twin.id)][$(reqdata.topic)] exec elapsed time: $elapsed secs"
        #end

        delete!(twin.sent, msg.id)
    else
        @debug "[$twin] unexpected response: $msg"
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
        @debug "[server] no provider for [$(msg.topic)]"
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

function get_router(broker="caronte")::Router
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

function publish(router::Router, topic, data)
    new_msg = PubSubMsg(topic, data)
    put!(router.process.inbox, Msg(TYPE_PUB, new_msg, Twin(router, "tmp")))
end

#=
Publish data on topic. Used by broker plugin module to republish messages
after transforming them.

The message is delivered by the twin.
=#
function publish(twin, topic, data)
    new_msg = PubSubMsg(topic, data)
    put!(twin.process.inbox, Msg(TYPE_PUB, new_msg, twin))
end

function pubsub_msg(router, twin, msg)
    if !isauthorized(router, twin, msg.topic)
        @warn "[$twin] is not authorized to publish on $(msg.topic)"
    else
        if (msg.flags & ACK_FLAG) == ACK_FLAG
            # reply with an ack message
            put!(twin.process.inbox, Msg(TYPE_ACK, AckMsg(msg.id), twin))
        end
        # msg is routable, get it to router
        pass = true
        if router.pub_handler !== nothing
            try
                pass = router.pub_handler(router.context, twin, msg)
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
    if twin.qos === with_ack
        msgid = msg.id
        if haskey(twin.acktimer, msgid)
            close(twin.acktimer[msgid])
            delete!(twin.acktimer, msgid)
            lock(twin.ack_cond[msgid]) do
                notify(twin.ack_cond[msgid])
            end
        end
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

#=
    twin_receiver(router, twin)

Receive messages from the client socket.
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

function challenge(router, twin, msg)
    if haskey(router.topic_function, "challenge")
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
        ws = twin.socket
        while isopen(ws)
            payload = transport_read(ws)
            if isempty(payload)
                twin.socket = nothing
                @debug "component [$(twin.id)]: connection closed"
                break
            end
            msg::RembusMsg = broker_parse(payload)
            #@mlog("[$(twin.id)] <- $(prettystr(msg))")
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
                twin = create_twin(string(bytes2zid(id)), router)
                @debug "[anonymous] client bound to twin id [$twin]"
                router.address2twin[id] = twin
                router.twin2address[twin.id] = id
                twin.socket = router.zmqsocket
            end

            msg::RembusMsg = broker_parse(router, pkt)
            #@mlog("[ZMQ][$twin] <- $(prettystr(msg))")

            if isa(msg, IdentityMsg)
                @debug "[$twin] auth identity: $(msg.cid)"
                # check if cid is registered
                rembus_login = isfile(key_file(router, msg.cid))
                if rembus_login
                    # authentication mode, send the challenge
                    response = challenge(router, twin, msg)
                else
                    identity_upgrade(router, twin, msg, id, authenticate=false)
                    continue
                end
                #@mlog("[ZMQ][$twin] -> $response")
                transport_send(twin, router.zmqsocket, response)
            elseif isa(msg, PingMsg)
                if (twin.id != msg.cid)

                    # broker restarted
                    # start the authentication flow if cid is registered
                    @debug "lost connection to broker: restarting $(msg.cid)"
                    rembus_login = isfile(key_file(router, msg.cid))
                    if rembus_login
                        # check if challenge was already sent
                        if !haskey(twin.session, "challenge")
                            response = challenge(router, twin, msg)
                            transport_send(twin, router.zmqsocket, response)
                        end
                    else
                        identity_upgrade(router, twin, msg, id, authenticate=false)
                    end

                else
                    if twin.socket !== nothing
                        pong(twin.socket, msg.id, id)

                        # check if there are cached messages
                        if twin.reactive
                            unpark(router.context, twin)
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
                #            elseif isa(msg, Remove)
                #                destroy_twin(twin, router)
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
    isa(e, WrongTcpPacket)
end

close_is_ok(::Nothing, e) = true

page_file(twin) = joinpath(twins_dir(twin.router), twin.id, string(twin.pager.ts))

#=
    detach(twin)

Disconnect the twin from the ws/tcp channel.
=#
function detach(twin)
    if twin.socket !== nothing
        if !isa(twin.socket, ZMQ.Socket)
            close(twin.socket)
        end
        twin.socket = nothing
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
                #@mlog("[$(twin.id)] -> $msg")
                transport_send(twin, twin.socket, msg, true)
            elseif isa(msg, EnableReactiveMsg)
                #@mlog("[$(twin.id)] -> $msg")
                transport_send(twin, twin.socket, ResMsg(msg.id, STS_SUCCESS, nothing), true)
                twin.router.unpark(twin.router.context, twin)
            else
                signal!(twin, msg)
            end
        end
    catch e
        @error "twin_task: $e" exception = (e, catch_backtrace())
        rethrow()
    finally
        cleanup(twin, twin.router)
        if isa(twin.socket, WebSockets.WebSocket)
            close(twin.socket, WebSockets.CloseFrameBody(1008, "unexpected twin close"))
        end
    end
    @debug "[$twin] task done"
end


#=
    handle_ack_timeout(tim, twin, msg, msgid)

Persist a PubSub message in case the acknowledge message is not received.
=#
function handle_ack_timeout(tim, twin, msg, msgid)
    if haskey(twin.acktimer, msgid)
        try
            twin.router.park(twin.router.context, twin, msg)
        catch e
            @error "[$twin] park (ack timeout): $e"
        end
    end
    delete!(twin.acktimer, msgid)

    if haskey(twin.ack_cond, msgid)
        lock(twin.ack_cond[msgid]) do
            notify(twin.ack_cond[msgid])
        end
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
        twin.router.park(twin.router.context, twin, msg.content)
        return nothing

    elseif twin.pager !== nothing && twin.pager.io !== nothing
        # it is online and reactive, send cached messages if any
        twin.router.unpark(twin.router.context, twin)
    end

    # current message
    if isa(msg.content, RpcReqMsg)
        # add to sent table
        twin.sent[msg.content.id] = SentData(time(), msg)
    end

    pkt = msg.content
    #@mlog "[$twin] -> $pkt"
    try
        transport_send(twin, twin.socket, pkt)
    catch e
        @error "[$twin] going offline: $e"
        @showerror e
        if msg.ptype === TYPE_PUB
            twin.router.park(twin.router.context, twin, msg.content)
        end
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
        cb(router.context, router)
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
            cb(router.context, router, twin, msg)
        end
        fn()
    catch e
        @error "$cb callback error: $e"
    end
end

function command_line()
    s = ArgParseSettings()
    @add_arg_table! s begin
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
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
    end
    return parse_args(s)
end

function caronte_reset(broker_name="caronte")
    rm(twins_dir(broker_name), force=true, recursive=true)
    bdir = broker_dir(broker_name)
    if isdir(bdir)
        foreach(rm, filter(isfile, readdir(bdir, join=true)))
    end
end

"""
    caronte(; wait=true, plugin=nothing, context=nothing, args=Dict())

Start the broker.

Return immediately when `wait` is false, otherwise blocks until shutdown is requested.

Overwrite command line arguments if args is not empty.
"""
function caronte(; wait=true, plugin=nothing, context=nothing, args=Dict())
    if isempty(args)
        args = command_line()
    end
    sv_name = get(args, "broker", "caronte")
    setup(CONFIG)
    if haskey(args, "debug") && args["debug"] === true
        CONFIG.debug = true
    end

    if haskey(args, "reset") && args["reset"] === true
        Rembus.caronte_reset(sv_name)
    end

    issecure = get(args, "secure", false)

    router = Router(plugin, context)

    tasks = [supervisor("twins", terminateif=:shutdown), process(broker, args=(router,))]

    if get(args, "http", nothing) !== nothing
        push!(
            tasks,
            process(
                serve_http,
                args=(router, args["http"], issecure),
                restart=:transient
            )
        )
    end
    if get(args, "tcp", nothing) !== nothing
        push!(
            tasks,
            process(
                serve_tcp,
                args=(router, args["tcp"], issecure),
                restart=:transient
            )
        )
    end
    if get(args, "zmq", nothing) !== nothing
        push!(
            tasks,
            process(
                serve_zeromq,
                args=(router, args["zmq"]),
                restart=:transient,
                debounce_time=2)
        )
    end
    if get(args, "ws", nothing) !== nothing ||
       (get(args, "zmq", nothing) === nothing && get(args, "tcp", nothing) === nothing)
        wsport = get(args, "ws", nothing)
        if wsport === nothing
            wsport = parse(UInt16, get(ENV, "BROKER_WS_PORT", "8000"))
        end
        push!(
            tasks,
            process(
                serve_ws,
                args=(router, wsport, issecure),
                restart=:transient,
                stop_waiting_after=2.0)
        )
    end

    sv = supervise(
        [supervisor(sv_name, tasks, strategy=:one_for_all, intensity=1)],
        wait=wait
    )
    return sv
end

function caronted()::Cint
    caronte()
    return 0
end

"""
    serve(server::Embedded; wait=true, secure=false)

Start an embedded server and accept connections.
"""
function serve(
    server::Embedded; wait=true, args=Dict()
)
    if isempty(args)
        args = command_line()
    end

    provide(server, "version", (ctx, cmp) -> VERSION)

    name = get(args, "name", "server")
    port = get(args, "ws", nothing)
    if port === nothing
        port = parse(UInt16, get(ENV, "BROKER_WS_PORT", "8000"))
    end

    secure = get(args, "secure", false)

    embedded_sv = from(name)

    if embedded_sv === nothing
        # first server process
        setup(CONFIG)
        if haskey(args, "debug") && args["debug"] === true
            CONFIG.debug = true
        end

        init_log()
        tasks = [
            supervisor("twins", terminateif=:shutdown),
            process(
                "serve:$port",
                serve_ws,
                args=(server, port, secure),
                restart=:transient,
                stop_waiting_after=2.0
            ),
        ]
        supervise(
            [supervisor(name, tasks, strategy=:one_for_one)],
            intensity=5,
            wait=wait
        )
    else
        p = process(
            "serve:$port",
            serve_ws,
            args=(server, port, secure),
            restart=:transient,
            stop_waiting_after=2.0
        )
        Visor.add_node(embedded_sv, p)
        Visor.start(p)
    end
    server.process = from(name)
    server.owners = load_owners(server)
    server.component_owner = load_token_app(server)
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

function client_receiver(router::Router, sock)
    cid = string(uuid4())
    twin = create_twin(cid, router)
    @debug "[anonymous] client bound to twin id [$cid]"

    # start the trusted client task
    twin.socket = sock

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
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "empty cid"), twin)
        return nothing
    end
    named = named_twin(msg.cid, router)
    if named !== nothing && !offline(named)
        @warn "a component with id [$(msg.cid)] is already connected"
        respond(router, ResMsg(msg.id, STS_GENERIC_ERROR, "already connected"), twin)
    else
        # check if cid is registered
        rembus_login = isfile(key_file(router, msg.cid))
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
    @debug "[server] client bound to twin id [$cid]"
    # start the trusted client task
    twin.socket = ws
    while isopen(ws)
        try
            payload = transport_read(ws)
            #=
            # eventually needed for tcp sockets
            if isempty(payload)
                @debug "[$twin]: connection close"
                break
            end
            =#
            msg::RembusMsg = broker_parse(payload)
            #@mlog("[$twin] <- $(prettystr(msg))")

            if isa(msg, RpcReqMsg)
                rpc_request(router, twin, msg)
            elseif isa(msg, PubSubMsg)
                pubsub_msg(router, twin, msg)
            elseif isa(msg, Register)
                register(router, twin, msg)
            elseif isa(msg, Unregister)
                unregister(router, twin, msg)
            elseif isa(msg, IdentityMsg)
                auth_twin = identity_check(router, twin, msg, paging=false)
                if auth_twin !== nothing
                    twin = auth_twin
                end
            elseif isa(msg, Attestation)
                twin = attestation(router, twin, msg)
                # update Visor process id
                twin.process.id = msg.cid
            end
        catch e
            receiver_exception(router, twin, e)
            if isa(e, EOFError)
                break
            end
        end
    end
    end_receiver(twin)

    return nothing
end

function secure_config(router)
    trust_store = keystore_dir(router)

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

function listener(proc, caronte_port, router, sslconfig)
    IP = "0.0.0.0"
    server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
    router.ws_server = server
    proto = (sslconfig === nothing) ? "ws" : "wss"
    @info "caronte up and running at port $proto:$caronte_port"

    setphase(proc, :listen)

    return HTTP.WebSockets.listen!(
        IP,
        caronte_port,
        server=server,
        sslconfig=sslconfig
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
        file = key_file(router, cid)

        if isfile(file)
            error("authentication failed")
        end
    else
        cid = val[1:idx-1]
        pwd = val[idx+1:end]
        file = key_file(router, cid)
        if isfile(file)
            secret = readline(file)
            if secret != pwd
                error("authentication failed")
            end
        else
            error("authentication failed")
        end
    end
    return cid
end

function authenticate(router::Router, req::HTTP.Request)
    auth = HTTP.header(req, "Authorization")

    if auth !== ""
        cid = verify_basic_auth(router, auth)
    else
        cid = string(uuid4())
    end

    return cid
end

function authenticate_admin(router::Router, req::HTTP.Request)
    cid = authenticate(router, req)
    if !(cid in router.admins)
        error("$cid authentication failed")
    end

    return cid
end

function command(router::Router, req::HTTP.Request, cmd::Dict)
    sts = 403
    cid = HTTP.getparams(req)["cid"]
    topic = HTTP.getparams(req)["topic"]
    twin = create_twin(cid, router)
    twin.hasname = true
    twin.socket = Condition()
    msg = AdminReqMsg(topic, cmd)
    admin_msg(router, twin, msg)
    response = wait(twin.socket)
    if response.status == 0
        sts = 200
    end
    return HTTP.Response(sts, [])
end

function http_subscribe(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => SUBSCRIBE_CMD))
end

function http_unsubscribe(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNSUBSCRIBE_CMD))
end

function http_expose(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => EXPOSE_CMD))
end

function http_unexpose(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNEXPOSE_CMD))
end

function http_publish(router::Router, req::HTTP.Request)
    try
        cid = authenticate(router, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end
        twin = create_twin(cid, router)
        msg = PubSubMsg(topic, content)
        pubsub_msg(router, twin, msg)
        return HTTP.Response(200, [])
    catch e
        @error "http::publish: $e"
        return HTTP.Response(403, [])
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
        twin = create_twin(cid, router)
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

        return HTTP.Response(
            sts,
            ["Content_type" => "application/json"],
            JSON3.write(retval)
        )
    catch e
        @error "http::rpc: $e"
        return HTTP.Response(403, [])
    end
end

function http_admin_command(
    router::Router, req::HTTP.Request, cmd::Dict, topic="__config__"
)
    try
        cid = authenticate_admin(router, req)
        twin = create_twin(cid, router)
        twin.socket = Condition()
        msg = AdminReqMsg(topic, cmd)
        admin_msg(router, twin, msg)
        response = wait(twin.socket)
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
        #showerror(stdout, e, stacktrace())
        return HTTP.Response(403, [])
    end
end

function http_admin_command(router::Router, req::HTTP.Request)
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

function http_private_topic(router::Router, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(router, req, Dict(COMMAND => PRIVATE_TOPIC_CMD), topic)
end

function http_public_topic(router::Router, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(router, req, Dict(COMMAND => PUBLIC_TOPIC_CMD), topic)
end

function http_authorize(router::Router, req::HTTP.Request)
    topic = HTTP.getparams(req)["topic"]
    return http_admin_command(
        router,
        req,
        Dict(COMMAND => AUTHORIZE_CMD, CID => HTTP.getparams(req)["cid"]),
        topic
    )
end

function http_unauthorize(router::Router, req::HTTP.Request)
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

function serve_http(td, router, port, issecure=false)
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

    try
        sslconfig = nothing
        if issecure
            sslconfig = secure_config(router)
        end

        router.http_server = HTTP.serve!(http_router, ip"0.0.0.0", port, sslconfig=sslconfig)
        for msg in td.inbox
            if isshutdown(msg)
                break
            end
        end
    finally
        @info "[serve_http] closed"
        setphase(td, :terminate)
        isdefined(router, :http_server) && close(router.http_server)
    end
end

function serve_ws(td, router, port, issecure=false)
    @debug "[serve_ws] starting"
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
        setphase(td, :terminate)
        isdefined(router, :ws_server) && close(router.ws_server)
    end
end

function serve_zeromq(pd, router, port)
    @debug "[serve_zeromq] starting"
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
        @debug "[serve_zeromq] closed"
    end
end

function serve_tcp(pd, router, caronte_port, issecure=false)
    proto = "tcp"
    server = nothing
    try
        IP = "0.0.0.0"
        setphase(pd, :listen)

        if issecure
            proto = "tls"
            sslconfig = secure_config(router)
        end

        server = Sockets.listen(Sockets.InetAddr(parse(IPAddr, IP), caronte_port))
        router.server = server
        @info "caronte up and running at port $proto:$caronte_port"
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
        setphase(pd, :terminate)
        server !== nothing && close(server)
    end
end

function islistening(
    wait=5; procs=["caronte.serve_ws", "caronte.serve_tcp", "caronte.serve_zeromq"]
)
    tcount = 0
    while tcount < wait
        if all(p -> getphase(p) === :listen, from.(procs))
            return true
        end
        tcount += 0.2
    end

    return false
end

isconnected(twin) = twin.socket !== nothing && isopen(twin.socket)

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
            while (target === nothing) && (current_index < len)
                current_index += 1
                impl = implementors[current_index]
                !isconnected(impl) && continue
                target = impl
                router.last_invoked[topic] = current_index
            end
            @info "[$topic]: no exposers available"
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
    @debug "[$topic] balancer: $(CONFIG.balancer)"
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
        @debug "no broadcast for [$msg]: request data not available or server method"
        return nothing
    end

    union!(authtwins, get(router.topic_interests, topic, Set{Twin}()))
    newmsg = Msg(TYPE_PUB, bmsg, msg.twchannel)

    for tw in authtwins
        @debug "broadcasting $topic to $(tw.id): [$newmsg]"
        put!(tw.process.inbox, newmsg)
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
            if isa(msg.data, Base.GenericIOBuffer)
                payload = dataframe_if_tagvalue(decode(msg.data))
            else
                payload = msg.data
            end
        end
        try
            result = router.topic_function[msg.topic](router.context, twin, getargs(payload)...)
            sts = STS_SUCCESS
        catch e
            @debug "[$(msg.topic)] server error (method too young?): $e"
            result = "$e"
            sts = STS_METHOD_EXCEPTION

            if isa(e, MethodError)
                try
                    result = Base.invokelatest(
                        router.topic_function[msg.topic],
                        router.context,
                        twin,
                        getargs(payload)...
                    )
                    sts = STS_SUCCESS
                catch e
                    result = "$e"
                end
            end
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
        router.topic_function["uptime"] = (ctx, session) -> uptime(router)
        router.topic_function["version"] = (ctx, session) -> Rembus.VERSION

        for msg in self.inbox
            # process control messages
            !isshutdown(msg) || break

            @debug "[broker] recv [type=$(msg.ptype)]: $msg"
            if isa(msg, Msg)
                if msg.ptype == TYPE_PUB
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
                end
            else
                @warn "unknown message: $msg"
            end
        end
    catch e
        @error "[broker] error: $e"
        rethrow()
    finally
        for tw in values(router.id_twin)
            save_page(tw)
        end
        save_configuration(router)
    end
    @debug "[broker] done"
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

    twin_dir = twins_dir(router)
    if !isdir(twin_dir)
        mkdir(twin_dir)
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
        if isdefined(router.plugin, :park) &&
           isdefined(router.plugin, :unpark)
            router.park = getfield(router.plugin, :park)
            router.unpark = getfield(router.plugin, :unpark)
        end

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

function ws_connect(
    egress::Visor.Process, twin::Twin, broker::Component, isconnected::Condition
)
    try
        url = brokerurl(broker)
        uri = URI(url)

        if uri.scheme == "wss"

            if !haskey(ENV, "HTTP_CA_BUNDLE")
                ENV["HTTP_CA_BUNDLE"] = joinpath(rembus_dir(), "ca", REMBUS_CA)
            end

            HTTP.WebSockets.open(socket -> begin
                    twin.socket = socket
                    notify(isconnected)
                    twin_receiver(twin.router, twin)
                    put!(egress.inbox, "connection closed")
                end, url)
        elseif uri.scheme == "ws"
            HTTP.WebSockets.open(socket -> begin
                    twin.socket = socket
                    notify(isconnected)
                    twin_receiver(twin.router, twin)
                    put!(egress.inbox, "connection closed")
                end, url, idle_timeout=1, forcenew=true)
        else
            error("ws endpoint: wrong $(uri.scheme) scheme")
        end
    catch e
        notify(isconnected, e, error=true)
        @showerror e
    end
end

function wait_response(twin::Twin, msg::Msg, timeout)
    mid::UInt128 = msg.content.id
    resp_cond = Threads.Condition()
    twin.out[mid] = resp_cond
    t = Timer((tim) -> response_timeout(resp_cond, msg.content), timeout)
    signal!(twin, msg)
    lock(resp_cond)
    res = wait(resp_cond)
    unlock(resp_cond)
    close(t)
    delete!(twin.out, mid)
    return res
end

function egress_task(proc, twin::Twin, broker::Component)
    isconnected = Condition()
    t = Timer((tim) -> connect_timeout(twin, isconnected), connect_request_timeout())
    @async ws_connect(proc, twin, broker, isconnected)
    wait(isconnected)
    close(t)
    msg = IdentityMsg(broker.id)
    wait_response(twin, Msg(TYPE_IDENTITY, msg, twin), request_timeout())

    # The context to pass to the plugin callbacks.
    twin.router.context = twin.socket

    msg = take!(proc.inbox)
    if isshutdown(msg)
        # close the connection
        close(twin.socket)
    else
        # the only message is an error condition
        error(msg)
    end
    @debug "[$proc] egress done"
end

#=
Connect this broker as a component to broker extracted from remote_url.
=#
function egress(
    remote_url::AbstractString, broker_name::AbstractString="caronte"
)
    proc = from("$broker_name.broker")

    # setup the twin
    router = proc.args[1]
    broker = Component(remote_url)
    twin = create_twin(broker.id, router)
    twin.hasname = true
    twin.pager = Pager(twin)

    # start the egress process
    startup(
        proc.supervisor.supervisor,
        process(remote_url, egress_task, args=(twin, broker), debounce_time=6)
    )

    return nothing
end

#=
The twin send an admin request to the peer broker.
=#
function transport_send(::Twin, ws, msg::AdminReqMsg)
    pkt = [TYPE_ADMIN | msg.flags, id2bytes(msg.id), msg.topic, msg.data]
    transport_write(ws, pkt)
end
