using Rembus

# Implements a Key Expressions Language similar to zenoh:
# https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md

struct SpaceTwin
    space::String
    twin::Twin
end


mutable struct KeySpaceRouter <: Rembus.AbstractRouter
    # r"house/.*/temperature" => ["house/*/temperature" => twin]
    spaces::Dict{Regex,Set{SpaceTwin}}
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function KeySpaceRouter()
        ksrouter = new(Dict(), nothing, nothing)
        ksrouter.process = process("ksrouter", zenoh_task, args=(ksrouter,))
        return ksrouter
    end
end

function build_space_re(topic)
    str = replace(topic, "/**/" => "(.*)", "**" => "(.*)", "*" => "([^/]+)")
    return Regex("^$str\$")

end

function subscribe_handler(ksrouter, msg)
    component = msg.twin
    topic = msg.topic
    @debug "[$component][subscribe] ksrouter:$ksrouter, topic:$topic"

    if contains(topic, "*")
        retopic = build_space_re(topic)
        if haskey(ksrouter.spaces, retopic)
            push!(ksrouter.spaces[retopic], SpaceTwin(topic, component))
        else
            ksrouter.spaces[retopic] = Set([SpaceTwin(topic, component)])
        end
    end
end

function unsubscribe_handler(ksrouter, msg)
    component = msg.twin
    topic = msg.topic
    @debug "[$component][unsubscribe] ksrouter:$ksrouter, topic:$topic"

    if contains(topic, "*")
        retopic = build_space_re(topic)
        if haskey(ksrouter.spaces, retopic)
            delete!(ksrouter.spaces[retopic], SpaceTwin(topic, component))
        end
    end
end

function publish_interceptor(ksrouter, msg)
    component = msg.twin
    @debug "[$component]: pub: $msg ($(msg.data))"
    topic = msg.topic

    if (contains(topic, "/"))
        for space_re in keys(ksrouter.spaces)
            m = match(space_re, topic)
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
                    for st in ksrouter.spaces[space_re]
                        @debug "[ksrouter] sending $msg to $(st.twin)"
                        publish(st.twin, st.space, topic, Rembus.msgdata(msg.data)...)
                    end
                end
            end
        end
    end
end

function zenoh_task(self, router)
    for msg in self.inbox
        @debug "[ksrouter] recv: $msg"
        !isshutdown(msg) || break

        if isa(msg, Rembus.AdminReqMsg) && haskey(msg.data, Rembus.COMMAND)
            if msg.data[Rembus.COMMAND] === Rembus.SUBSCRIBE_CMD
                @debug "[ksrouter][$(msg.twin)] subscribing: $msg"
                subscribe_handler(router, msg)
            elseif msg.data[Rembus.COMMAND] === Rembus.UNSUBSCRIBE_CMD
                @debug "[ksrouter][$(msg.twin)] unsubscribing: $msg"
                unsubscribe_handler(router, msg)
            end
        elseif isa(msg, Rembus.PubSubMsg)
            publish_interceptor(router, msg)
        end

        # route to downstream broker
        put!(router.downstream.process.inbox, msg)
    end
end
