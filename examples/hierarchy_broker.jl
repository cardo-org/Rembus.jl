using Rembus

# Implements a Key Expressions Language similar to zenoh:
# https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md

mutable struct Zenoh <: Rembus.AbstractRouter
    # r"house/.*/temperature" => ["house/*/temperature" => twin]
    spaces::Dict{Regex,Vector{Pair{String,Rembus.Twin}}}
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function Zenoh()
        return new(Dict(), nothing, nothing)
    end
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

function subscribe_handler(zenoh, msg)
    component = msg.twin
    topic = msg.topic
    @debug "[$component][subscribe] zenoh:$zenoh, topic:$topic"

    if contains(topic, "*")
        str = replace(topic, "/**/" => "(.*)", "**" => "(.*)", "*" => "([^/]+)")
        retopic = Regex("^$str\$")
        if haskey(zenoh.spaces, retopic)
            push!(zenoh.spaces[retopic], Pair(topic, component))
        else
            zenoh.spaces[retopic] = [Pair(topic, component)]
        end
    end
end

function publish_interceptor(zenoh, msg)
    component = msg.twin
    @debug "[$component]: pub: $msg ($(msg.data))"
    topic = msg.topic

    if (contains(topic, "/"))
        for space in keys(zenoh.spaces)
            m = match(space, topic)
            if m !== nothing
                unsealed = true
                for capture in m.captures
                    if contains(capture, "@")
                        # the regex topic chunck matched a verbatim chunck
                        # topic does not match the regex
                        unsealed = false
                        break
                    end
                end

                if unsealed
                    for cmp in zenoh.spaces[space]
                        @debug "[zenoh] sending: $payload"
                        publish(cmp.second, cmp.first, topic, msg_payload(msg.data)...)
                    end
                end
            end
        end
    end
end

function zenoh_task(self, router)
    for msg in self.inbox
        @debug "[zenoh] recv: $msg"
        !isshutdown(msg) || break

        if isa(msg, Rembus.AdminReqMsg) &&
           haskey(msg.data, Rembus.COMMAND) &&
           msg.data[Rembus.COMMAND] === Rembus.SUBSCRIBE_CMD
            @debug "[zenoh][$(msg.twin)] subscribing: $msg"
            subscribe_handler(router, msg)
        elseif isa(msg, Rembus.PubSubMsg)
            publish_interceptor(router, msg)
        end

        # route to downstream broker
        put!(router.downstream.process.inbox, msg)
    end
end

function start_zenoh(supervisor_name, downstream_router)
    zenoh = Zenoh()
    Rembus.upstream!(downstream_router, zenoh)
    sv = from(supervisor_name)
    zenoh.process = process("zenoh", zenoh_task, args=(zenoh,))
    startup(sv, zenoh.process)
end

function start_broker()
    router = Rembus.get_router(name="z", ws=8000)
    start_zenoh("z", router)
    return Rembus.bind(router)
end

rb = start_broker()
wait(rb)
