
# COV_EXCL_START

always_true(uid) = true

isenabled(router, tenant::Nothing) = true

# COV_EXCL_STOP

stacktrace!(enable=true) = set_preferences!(Rembus, "stacktrace" => enable, force=true)

function dumperror(e)
    if get(getcfg(), "stacktrace", false)
        showerror(stdout, e, catch_backtrace())
    end
end

getcfg() = get(Base.get_preferences(), "Rembus", Dict())

function default_rembus_dir()
    if Sys.iswindows()
        home = get(ENV, "USERPROFILE", ".")
    else
        home = get(ENV, "HOME", ".")
    end
    return joinpath(home, ".config", "rembus")
end

function rembus_dir()
    cfg = getcfg()
    get(cfg, "rembus_dir", get(ENV, "REMBUS_DIR", default_rembus_dir()))
end

function rembus_dir!(new_dir::AbstractString)
    set_preferences!(Rembus, "rembus_dir" => new_dir, force=true)
end

function init_log(level=nothing)
    if !haskey(ENV, "JULIA_DEBUG")
        isnothing(level) ? logging("warn") : logging(level)
    end

    return nothing
end

debug!() = init_log("debug")
info!() = init_log("info")
warn!() = init_log("warn")
error!() = init_log("error")

anonymous!() = set_preferences!(
    Rembus, "connection_mode" => string(anonymous), force=true
)

authenticated!() = set_preferences!(
    Rembus, "connection_mode" => string(authenticated), force=true
)

#=
Get the message data payload.
Useful for content filtering by publish_interceptor().
=#
function msgdata(io::IOBuffer)
    mark(io)
    payload = decode(io)
    reset(io)
    return payload
end

msgdata(v) = v # COV_EXCL_LINE

function string_to_enum(connection_mode)
    if connection_mode == "anonymous"
        return anonymous
    elseif connection_mode == "authenticated"
        return authenticated
    else
        error("invalid connection_mode [$connection_mode]")
    end
end

"""
    request_timeout()

Returns the default request timeout used when creating new nodes with the
[`broker`](@ref), [`component`](@ref), [`connect`](@ref), or [`server`](@ref) functions.
"""
function request_timeout()
    cfg = getcfg()
    get(cfg, "request_timeout", parse(Float64, get(ENV, "REMBUS_TIMEOUT", "5")))
end

"""
    request_timeout!(value::Real)

Set the default request timeout used when creating new nodes with the
[`broker`](@ref), [`component`](@ref), [`connect`](@ref), or [`server`](@ref) functions.
"""
function request_timeout!(value::Real)
    set_preferences!(Rembus, "request_timeout" => value, force=true)
end

function challenge_timeout!(newval)
    set_preferences!(Rembus, "challenge_timeout" => newval, force=true)
end

function ack_timeout!(newval)
    set_preferences!(Rembus, "ack_timeout" => newval, force=true)
end

function ws_ping_interval!(newval)
    set_preferences!(Rembus, "ws_ping_interval" => newval, force=true)
end

function zmq_ping_interval!(newval)
    set_preferences!(Rembus, "zmq_ping_interval" => newval, force=true)
end

#=
Return a copy of `msg` modified by function `fn`.
=#
function copy_from(fn, msg)
    result = deepcopy(msg)
    fn(result)
    return result
end

function spliturl(url::String)
    baseurl = get(ENV, "REMBUS_BASE_URL", "ws://127.0.0.1:8000")
    baseuri = URI(baseurl)
    uri = URI(url)
    props = queryparams(uri)

    host = uri.host
    if host == ""
        host = baseuri.host
    end

    portstr = uri.port
    if portstr == ""
        portstr = baseuri.port
    end

    port = parse(UInt16, portstr)

    proto = uri.scheme
    if proto == ""
        name = uri.path
        protocol = Symbol(baseuri.scheme)
    elseif proto in ["ws", "wss", "tcp", "tls", "zmq"]
        name = startswith(uri.path, "/") ? uri.path[2:end] : uri.path
        protocol = Symbol(proto)
    else
        error("wrong url $url: unknown protocol $proto")
    end
    if isempty(name)
        name = string(uuid4())
        hasname = false
    else
        hasname = true
    end
    return (name, hasname, protocol, host, port, props)
end

function to_microseconds(msg_from::Union{Real,Period,Dates.CompoundPeriod})
    if isa(msg_from, Real)
        return msg_from
    elseif isa(msg_from, Period)
        return Microsecond(msg_from).value
    elseif isa(msg_from, Dates.CompoundPeriod)
        return sum(Microsecond.(msg_from.periods)).value
    end
end

function secure_config(router)
    trust_store = keystore_dir()
    @debug "[$router] keystore: $trust_store"

    entropy = MbedTLS.Entropy()
    rng = MbedTLS.CtrDrbg()
    MbedTLS.seed!(rng, entropy)

    sslconfig = MbedTLS.SSLConfig(
        joinpath(trust_store, "rembus.crt"),
        joinpath(trust_store, "rembus.key")
    )
    MbedTLS.rng!(sslconfig, rng)

    ## Example code snippet for debugging connection issues.
    #=
        function show_debug(level, filename, number, msg)
            @show level, filename, number, msg
        end

        MbedTLS.dbg!(sslconfig, show_debug)
    =#
    return sslconfig
end

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
    if (args isa Vector) || (args isa Tuple)
        return args
    elseif args === nothing
        return []
    else
        return [args]
    end
end

function set_policy(router, policy)
    if !(policy in ["first_up", "less_busy", "round_robin"])
        error("wrong routing policy, must be one of first_up, less_busy, round_robin")
    end

    router.policy = Symbol(policy)

    return nothing
end

#=
    callback_or(fn::Function, router::AbstractRouter, callback::Symbol)

Invoke `callback` function if it is injected via the plugin module otherwise invoke `fn`.
=#
function callback_or(fn::Function, router::Router, callback::Symbol)
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
    fn::Function, cb::Symbol, router::Router, twin::Twin, msg::RembusMsg
)
    try
        if router.plugin !== nothing && isdefined(router.plugin, cb)
            cb = getfield(router.plugin, cb)
            cb(router.shared, router, twin, msg)
        end
        fn()
    catch e
        @error "[$twin] $cb callback error: $e"
    end
end

isbuiltin(fn) = fn in ["rid", "version", "uptime"]

function router_configuration(router)
    cfg = Dict("exposers" => Dict(), "subscribers" => Dict())
    for (topic, twins) in router.topic_impls
        cfg["exposers"][topic] = [rid(t) for t in twins if !isbuiltin(topic)]
    end
    for (topic, twins) in router.topic_interests
        cfg["subscribers"][topic] = [rid(t) for t in twins]
    end

    return cfg
end

function twin_configuration(router, twin)
    cfg = Dict("exposers" => [], "subscribers" => [])
    for topic in keys(router.topic_function)
        isbuiltin(topic) && continue
        if haskey(router.subinfo, topic)
            push!(cfg["subscribers"], topic)
        else
            push!(cfg["exposers"], topic)
        end
    end

    return cfg
end

function twin_setup(router, twin)
    cfg::Dict{String,Any} = twin_configuration(router, twin)
    cfg[COMMAND] = SETUP_CMD
    msg = AdminReqMsg(
        twin,
        BROKER_CONFIG,
        cfg,
        rid(twin)
    )
    response = send_msg(twin, msg)
    result = fetch(response.future)
    close(response.timer)
    return (result.status === STS_SUCCESS)
end

#=
Methods related to the persistence of Pubsub messages.
=#

messages_fn(router, ts) = joinpath(messages_dir(router), string(ts))

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
    Save pubsub message to in-memory cache and return the message pointer.

    For QOS1 or QOS2 levels the message id is used to match ACK and ACK2 messages.
=#
function save_message(router, msg::PubSubMsg)
    tv = Libc.TimeVal()
    ts = tv.sec * 1_000_000 + tv.usec
    if isa(msg.data, IOBuffer)
        data = msg.data.data
    else
        data = encode_message(msg)
    end
    router.mcounter += 1

    #if isempty(filter(:uid => ==(msg.id), router.msg_df))
    push!(router.msg_df, [router.mcounter, ts, msg.id, msg.topic, data])
    #end

    if (router.mcounter % router.settings.db_max_messages) == 0
        persist_messages(router)
        @debug "persisted $(router.mcounter) file"
        router.msg_df = msg_dataframe()
    end

    return router.mcounter
end

function get_data(pkt)
    payload = decode(pkt)
    ptype = payload[1] & 0x0f
    flags = payload[1] & 0xf0
    if ptype == TYPE_PUB
        if flags > QOS0
            data = dataframe_if_tagvalue(payload[4])
        else
            data = dataframe_if_tagvalue(payload[3])
        end
    end

    return data
end

function data_at_rest(; from=LastReceived, broker="broker")
    files = messages_files(broker, to_microseconds(from))
    result = DataFrame(
        ptr=UInt64[],
        ts=UInt64[],
        uid=UInt128[],
        topic=String[],
        pkt=Vector{UInt8}[],
        data=Any[]
    )
    for fn in files
        path = joinpath(messages_dir(broker), fn)
        if isfile(path)
            df = load_object(path)
            df.data = get_data.(Vector{UInt8}.(df.pkt))
            result = vcat(result, df)
        end
    end
    return result
end

function send_messages(twin::Twin, df)
    nowts = time() * 1_000_000
    for row in eachrow(df)
        tmark = twin.mark
        if row.ptr > tmark
            if haskey(twin.msg_from, row.topic) &&
               row.ts > (nowts - twin.msg_from[row.topic])
                Rembus.from_cbor(twin, row.ptr, row.pkt)
            end
        end
    end
end

#=
Return the subscribed pubsub topics of the twin
=#
function twin_topics(twin::Twin)
    router = last_downstream(twin.router)
    topics = []
    for (k, v) in router.topic_interests
        if twin in v
            push!(topics, k)
        end
    end
    return topics
end

function from_disk_messages(twin::Twin, fn)
    path = joinpath(messages_dir(twin.router), fn)
    df = load_object(path)
    interests = twin_topics(twin)
    filtered = df[findall(el -> ismissing(el) ? false : el in interests, df.topic), :]
    if !isempty(filtered)
        filtered.msg = decode.(Vector{UInt8}.(filtered.pkt))
        send_messages(twin, filtered)
    end
end

function from_memory_messages(twin::Twin)
    #@debug "[$twin] in-memory messages df:\n$(twin.router.msg_df)"
    router = last_downstream(twin.router)
    send_messages(twin, router.msg_df)
end

file_lt(f1, f2) = parse(Int, f1) < parse(Int, f2)

function msg_files(router)
    return sort(readdir(Rembus.messages_dir(router)), lt=file_lt)
end

msg_files(twin::Twin) = msg_files(twin.router)

function persist_messages(router)
    fn = messages_fn(router, router.mcounter)
    @debug "[broker] persisting messages on disk: $fn"

    save_object(fn, router.msg_df)
end

function broker_reset(broker_name="broker")
    rm(messages_dir(broker_name), force=true, recursive=true)
    bdir = broker_dir(broker_name)
    if isdir(bdir)
        foreach(rm, filter(isfile, readdir(bdir, join=true)))
    end
end

probe!(twin::Twin) = twin.probe = true

unprobe!(twin::Twin) = twin.probe = false

function reset_probe!(twin::Twin)
    empty!(probeCollector)
    probe!(twin)
end

function probe_add(msg::RembusMsg, dir::InOut)
    if isdefined(msg, :twin)
        twin = msg.twin
        twin.probe && probe_add(twin, msg, dir)
    end
end

function probe_add(twin::Twin, msg::RembusMsg, dir::InOut)
    key = path(twin)
    if !haskey(probeCollector, key)
        probeCollector[key] = []
    end
    push!(probeCollector[key], ProbedMsg(Libc.TimeVal(), dir, msg))
end

function probe_inspect(twin::Twin)::Vector{ProbedMsg}
    key = path(twin)
    if haskey(probeCollector, key)
        return probeCollector[key]
    end
    return Vector{ProbedMsg}[]
end

micros(t::Libc.TimeVal) = t.sec * 1_000_000 + t.usec

function probe_pprint(twin::Twin)
    probed_messages = probe_inspect(twin)
    report = "$(path(twin)):\n"
    index = 0
    start_ts = 0
    msgs = map(probed_messages) do msg
        index += 1
        if start_ts == 0
            start_ts = micros(msg.ts)
        end
        delta = micros(msg.ts) - start_ts
        dir = msg.direction === pktin ? "<< " : ">> "
        "[$index][$(@sprintf("%010d", delta))] $dir $(typeof(msg.msg)) [$(msg.msg)]"
    end

    report *= join(msgs, "\n")
    println(report)
end
