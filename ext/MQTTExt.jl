module MQTTExt

using Mosquitto
using DataStructures
using JSON3
using Rembus

function __init__()
    push!(Rembus.Protocols, "mqtt")
end

struct MQTTSock <: Rembus.AbstractSocket
    sock::Mosquitto.Client_v5
    out::Dict{Rembus.Msgid,Rembus.FutureResponse}
    direct::Dict{Rembus.Msgid,Rembus.FutureResponse}
    MQTTSock(sock) = new(
        sock,
        Dict(),
        Dict()
    )
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

end

function bind_topic(rb::Rembus.Twin, client::Mosquitto.Client_v5)
    mqtt_cfg = get(rb.router.settings.ext, "mqtt", Dict())
    topic = get(mqtt_cfg, "subscribe_topic", "#")
    @debug "[$rb] subscribing to topic [$topic]"
    Mosquitto.subscribe(client, topic)
end

function Rembus.connect(rb::Rembus.Twin, ::Rembus.Adapter{:MQTT})
    @debug "[$rb] connecting to MQTT broker $(Rembus.cid(rb))"
    client = Client_v5(rb.uid.host, Int(rb.uid.port))
    add_subscription(rb.router, rb, "*")
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

    if length(data) == 1
        content = data[1]
        if !isa(content, String)
            content = JSON3.write(content)
        end
        Mosquitto.publish(socket.sock, msg.topic, content)
    elseif isempty(data)
        ## Available in Mosquitto.jl > 0.3.0
        # Mosquitto.publish(socket.sock, msg.topic, nothing)
    else
        @warn "MQTT: ignoring message with multiple data args"
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
        data = String(msg.payload)
        @debug "[$rb] topic: $(msg.topic) - msg: $data"

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
