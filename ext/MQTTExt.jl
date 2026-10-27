module MQTTExt

using Mosquitto
using DataStructures
using JSON3
using Rembus

function __init__()
    push!(Rembus.Protocols, "mqtt")
    push!(Rembus.Protocols, "mqtts")
end

struct MQTTSock <: Rembus.AbstractSocket
    sock::Mosquitto.Client_v5
    out::Dict{Rembus.Msgid,Rembus.FutureResponse}
    direct::Dict{Rembus.Msgid,Rembus.FutureResponse}
    # Messages published by the bridge that the MQTT broker is expected to
    # echo back: (topic, payload) => expiration times of the pending echoes.
    echoes::Dict{Tuple{String,String},Vector{Float64}}
    echoes_lock::ReentrantLock
    MQTTSock(sock) = new(
        sock,
        Dict(),
        Dict(),
        Dict(),
        ReentrantLock()
    )
end

# Time after which a published message is no more expected to be echoed back.
const ECHO_TTL = 10.0

function expect_echo!(socket::MQTTSock, topic, payload)
    now = time()
    lock(socket.echoes_lock) do
        for (k, v) in socket.echoes
            filter!(>(now), v)
            isempty(v) && delete!(socket.echoes, k)
        end
        push!(get!(socket.echoes, (topic, payload), Float64[]), now + ECHO_TTL)
    end
end

# Return true (and consume the pending echo) if the message is the echo of a
# message published by the bridge: it was already delivered to the local
# subscribers.
function consume_echo!(socket::MQTTSock, topic, payload)
    now = time()
    lock(socket.echoes_lock) do
        pending = get(socket.echoes, (topic, payload), nothing)
        pending === nothing && return false
        filter!(>(now), pending)
        isempty(pending) && (delete!(socket.echoes, (topic, payload)); return false)
        popfirst!(pending)
        isempty(pending) && delete!(socket.echoes, (topic, payload))
        return true
    end
end

Base.isopen(client::MQTTSock) = client.sock.conn_status[] === true

function Base.close(client::MQTTSock)
    disconnect(client.sock)
    Mosquitto.loop_stop(client.sock)
end

Rembus.requireauthentication(::MQTTSock) = false # COV_EXCL_LINE

function add_subscription(router, twin, topic)
    if !haskey(router.topic_interests, topic)
        router.topic_interests[topic] = OrderedSet{Rembus.Twin}()
    end
    push!(router.topic_interests[topic], twin)
    Rembus.mark_glob_topic!(router, topic)

end

function bind_topic(rb::Rembus.Twin, client::Mosquitto.Client_v5)
    mqtt_cfg = get(rb.router.settings.ext, "mqtt", Dict())
    topic = get(mqtt_cfg, "subscribe_topic", "#")
    @debug "[$rb] subscribing to topic [$topic]"
    Mosquitto.subscribe(client, topic)
end

# Configure TLS using the Rembus CA (HTTP_CA_BUNDLE or REMBUS_DIR/ca) and, if
# present, the certificate and key of the Rembus keystore as client credentials.
function tls_setup(client::Mosquitto.Client_v5)
    cafile = get(ENV, "HTTP_CA_BUNDLE") do
        Rembus.rembus_ca()
    end
    keystore = Rembus.keystore_dir()
    certfile = joinpath(keystore, "rembus.crt")
    keyfile = joinpath(keystore, "rembus.key")
    if isfile(certfile) && isfile(keyfile)
        flag = Mosquitto.tls_set(client, cafile; certfile=certfile, keyfile=keyfile)
    else
        flag = Mosquitto.tls_set(client, cafile)
    end
    flag != Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS &&
        error("MQTT TLS setup failed (cafile=$cafile), error $flag")
end

function Rembus.connect(rb::Rembus.Twin, ::Rembus.Adapter{:MQTT})
    @debug "[$rb] connecting to MQTT broker $(Rembus.cid(rb))"
    client = Client_v5()
    if Rembus.protocol(rb) === :mqtts
        tls_setup(client)
    end
    Mosquitto.connect(client, rb.uid.host, Int(rb.uid.port))
    add_subscription(rb.router, rb, "**")
    rb.reactive = true
    rb.socket = MQTTSock(client)
    bind_topic(rb, client)
    @async mqtt_receiver(rb)
end

function Rembus.transport_send(socket::MQTTSock, msg)
    @debug "MQTTSock: $msg to /dev/null"
    return true
end

function Rembus.transport_send(socket::MQTTSock, msg::Rembus.PubSubMsg)
    outcome = true
    @info "publishing to MQTT topic $(msg.topic)"
    if isa(msg.data, Base.GenericIOBuffer)
        data = Rembus.decode(copy(msg.data))
    else
        data = msg.data
    end

    if isempty(data)
        # 0-length payload, no content type
        Mosquitto.publish(socket.sock, msg.topic, UInt8[])
    else
        # A single argument is published as is, multiple arguments as a
        # JSON array.
        content = length(data) == 1 ? data[1] : data
        props = Mosquitto.PropertyList("content-type", "application/json")
        payload = JSON3.write(content)
        # A message published through the MQTT twin itself has not been
        # delivered locally: its echo is the only copy, do not drop it.
        if msg.twin.socket !== socket
            expect_echo!(socket, msg.topic, payload)
        end
        Mosquitto.publish(
            socket.sock,
            msg.topic,
            payload;
            properties=props
        )
    end

    return outcome
end

function onconnect(rb, client)
    ch = get_connect_channel(client)
    while true
        msg = take!(ch)
        @debug "MQTT connection event: val=$(msg.val) returncode=$(msg.returncode)"
        if msg.returncode == Mosquitto.MosquittoCwrapper.MOSQ_ERR_SUCCESS && msg.val == 1
            bind_topic(rb, client)
        end
    end
end

function mqtt_receiver(rb::Rembus.Twin)
    client = rb.socket.sock
    pd = rb.process
    msg_channel = get_messages_channel(client)
    Mosquitto.loop_start(client)

    @async onconnect(rb, client)
    @debug "[$rb] started mqtt receiver"

    while true
        msg = take!(msg_channel) # Tuple{String, Vector{UInt8})
        payload = String(msg.payload)
        @debug "[$rb] topic: $(msg.topic) - msg: $payload"

        # Skip the echo of a message published by this bridge, otherwise the
        # local subscribers would receive it twice.
        if consume_echo!(rb.socket, msg.topic, payload)
            @debug "[$rb] topic: $(msg.topic): echo dropped"
            continue
        end

        # The payload must be JSON: an array becomes the list of message
        # arguments, any other value a single argument.
        # An empty payload is a message without arguments.
        data = try
            if isempty(payload)
                []
            else
                parsed = Rembus.lazy_to_julia(JSON3.read(payload))
                parsed isa Vector ? parsed : [parsed]
            end
        catch e
            @error "[$rb] MQTT topic $(msg.topic): invalid JSON payload: $e"
            continue
        end

        # Send MQTT message to router
        put!(rb.router.process.inbox, Rembus.PubSubMsg(
            rb,
            msg.topic,
            data,
            0x0,
        ))
    end
end


end
