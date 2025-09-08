function verify_basic_auth(router, authorization)
    # See: https://datatracker.ietf.org/doc/html/rfc7617
    val = String(Base64.base64decode(replace(authorization, "Basic " => "")))
    idx = findfirst(':', val)

    if idx === nothing
        # no password, only component name
        cid = val
        if key_file(router, cid) !== nothing ||
           router.settings.connection_mode === authenticated
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

function authenticate(router::Router, req::HTTP.Request)
    auth_header = HTTP.header(req, "Authorization")
    isauth = false

    if auth_header !== ""
        cid = verify_basic_auth(router, auth_header)
        isauth = true
    else
        if router.mode === authenticated
            error("anonymous: authentication failed")
        else
            cid = string(uuid4())
        end
    end

    return (cid, isauth)
end

function authenticate_admin(router::Router, req::HTTP.Request)
    (cid, isauth) = authenticate(router, req)
    if !(cid in router.admins)
        error("$cid authentication failed")
    end

    return (cid, isauth)
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
    twin = bind(router, RbURL(cid))

    try
        msg = AdminReqMsg(twin, topic, cmd)
        response = http_admin_msg(router, twin, msg)
        if response.status == 0
            sts = 200
        end
    finally
        Visor.shutdown(twin.process)
        cleanup(twin, router)
    end

    return HTTP.Response(sts, ["Access-Control-Allow-Origin" => "*"])
end

function http_subscribe(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => SUBSCRIBE_CMD, TOUCHED => []))
end

function http_unsubscribe(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNSUBSCRIBE_CMD, TOUCHED => []))
end

function http_expose(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => EXPOSE_CMD, TOUCHED => []))
end

function http_unexpose(router::Router, req::HTTP.Request)
    return command(router, req, Dict(COMMAND => UNEXPOSE_CMD, TOUCHED => []))
end

function jsonrpc_response(sts, retval)
    headers = [
        "Content_type" => "application/json",
        "Access-Control-Allow-Origin" => "*"
    ]
    if isnothing(retval)
        return HTTP.Response(sts, headers)
    else
        return HTTP.Response(sts, headers, JSON3.write(retval))
    end
end

function lazy_to_julia(val)
    if val isa JSON3.Object
        return Dict{String,Any}(string(k) => lazy_to_julia(v) for (k, v) in val)
    elseif val isa JSON3.Array
        return [lazy_to_julia(v) for v in val]
    else
        return val
    end
end

function http_jsonrpc_parse(router::Router, twin::Twin, payload::String)
    pkt = lazy_to_julia(JSON3.read(payload))

    if pkt isa Dict
        msg = json_parse(twin, pkt)
        retval = http_jsonrpc_eval(router, twin, msg)
    elseif pkt isa Vector
        retval = []
        for obj in Vector(pkt)
            msg = json_parse(twin, obj)
            result = http_jsonrpc_eval(router, twin, msg)
            if result !== nothing
                push!(retval, result)
            end
        end
        if isempty(retval)
            retval = nothing
        end
    end
    return retval
end

function http_jsonrpc_eval(router::Router, twin::Twin, msg::RembusMsg)
    @debug "http_jsonrpc: msg=$msg"
    retval = nothing

    if isa(msg, RpcReqMsg)
        fut_response = fpc(twin, msg.topic, isnothing(msg.data) ? () : msg.data)
        response = fetch(fut_response.future)
        retval = Dict{String,Any}(
            "jsonrpc" => "2.0",
            "id" => msg.id
        )
        if response.status == 0
            retval["result"] = jsonrpc_response_data(response)
        else
            retval["error"] = Dict(
                "code" => -32000,
                "message" => jsonrpc_response_data(response)
            )
        end
    elseif isa(msg, PubSubMsg)
        pubsub_msg(router, msg)
    end

    return retval
end

function http_jsonrpc(router::Router, req::HTTP.Request)
    (cid, isauth) = authenticate(router, req)
    @debug "http_jsonrpc: cid=$cid isauth=$isauth"
    retval = Dict{String,Any}(
        "jsonrpc" => "2.0",
        "id" => missing
    )
    sts = 200
    if isempty(req.body)
        retval["error"] = Dict(
            "code" => -32600,
            "message" => "invalid JSON: empty content"
        )
        return jsonrpc_response(sts, retval)
    end

    content = String(req.body)

    if haskey(router.id_twin, cid)
        retval["error"] = Dict(
            "code" => -32000,
            "message" => "component $cid not available for rpc via http"
        )
    else
        twin = bind(router, RbURL(cid))
        twin.isauth = isauth
        twin.socket = Float()

        try
            retval = http_jsonrpc_parse(router, twin, content)
        catch e
            retval["error"] = Dict(
                "code" => -32000,
                "message" => string(e)
            )
        finally
            Visor.shutdown(twin.process)
            cleanup(twin, router)
        end
    end

    return jsonrpc_response(sts, retval)
end

function http_publish(router::Router, req::HTTP.Request)
    try
        (cid, isauth) = authenticate(router, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end
        if haskey(router.id_twin, cid)
            error("component $cid not available for publish via http")
        else
            twin = bind(router, RbURL(cid))
            twin.isauth = isauth
            msg = PubSubMsg(twin, topic, content)
            pubsub_msg(router, msg)
            Visor.shutdown(twin.process)
            cleanup(twin, router)
            return HTTP.Response(200, ["Access-Control-Allow-Origin" => "*"])
        end
    catch e
        @debug "http::publish: $e"
        return HTTP.Response(403, ["Access-Control-Allow-Origin" => "*"])
    end
end

function http_response_data(response)
    data = dataframe_if_tagvalue(response_data(response))
    if isa(data, DataFrame)
        return arraytable(data)
    else
        return JSON3.write(data)
    end
end

function jsonrpc_response_data(response)
    data = dataframe_if_tagvalue(response_data(response))
    if isa(data, DataFrame)
        return arraytable(data)
    else
        return data
    end
end

function http_rpc(router::Router, req::HTTP.Request)
    try
        (cid, isauth) = authenticate(router, req)
        topic = HTTP.getparams(req)["topic"]
        if isempty(req.body)
            content = []
        else
            content = JSON3.read(req.body, Any)
        end
        if haskey(router.id_twin, cid)
            error("component $cid not available for rpc via http")
        else
            twin = bind(router, RbURL(cid))
            twin.isauth = isauth
            twin.socket = Float()
            fut_response = fpc(twin, topic, content)
            response = fetch(fut_response.future)
            retval = http_response_data(response)
            if response.status == 0
                sts = 200
            else
                sts = 403
            end

            Visor.shutdown(twin.process)
            cleanup(twin, router)
            return HTTP.Response(
                sts,
                [
                    "Content_type" => "application/json",
                    "Access-Control-Allow-Origin" => "*"
                ],
                retval
            )
        end
    catch e
        @error "http::rpc: $e"
        return HTTP.Response(404, ["Access-Control-Allow-Origin" => "*"])
    end
end

function http_admin_command(
    router::Router, req::HTTP.Request, cmd::Dict, topic="__config__"
)
    try
        (cid, isauth) = authenticate_admin(router, req)
        twin = bind(router, RbURL(cid))
        twin.isauth = isauth
        msg = AdminReqMsg(twin, topic, cmd)
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

function http_admin_command(router::Router, req::HTTP.Request)
    cmd = HTTP.getparams(req)["cmd"]
    return http_admin_command(
        router, req, Dict(COMMAND => cmd)
    )
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

function _serve_http(td, router::Router, http_router, port, issecure)
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

function serve_http(td, router::Router, port, issecure=false)
    @info "[serve_http] starting at port $port"

    # define REST endpoints to dispatch rembus functions
    http_router = HTTP.Router()

    # json-rpc
    HTTP.register!(http_router, "POST", "/", req -> http_jsonrpc(router, req))

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
