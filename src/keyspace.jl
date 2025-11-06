using Rembus

# Implements a Key Expressions Language similar to ksrouter:
# https://github.com/eclipse-ksrouter/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md

mutable struct KeySpaceRouter <: Rembus.AbstractRouter
    # r"house/.*/temperature" => ["house/*/temperature" => twin]
    spaces::Dict{Regex,Vector{Pair{String,Rembus.Twin}}}
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function KeySpaceRouter()
        ksrouter = new(Dict(), nothing, nothing)
        ksrouter.process = process("ksrouter", zenoh_task, args=(ksrouter,))
        return ksrouter
    end
end

function subscribe_handler(ksrouter, msg)
    component = msg.twin
    topic = msg.topic
    @debug "[$component][subscribe] ksrouter:$ksrouter, topic:$topic"

    if contains(topic, "*")
        str = replace(topic, "/**/" => "(.*)", "**" => "(.*)", "*" => "([^/]+)")
        retopic = Regex("^$str\$")
        if haskey(ksrouter.spaces, retopic)
            push!(ksrouter.spaces[retopic], Pair(topic, component))
        else
            ksrouter.spaces[retopic] = [Pair(topic, component)]
        end
    end
end

function publish_interceptor(ksrouter, msg)
    component = msg.twin
    @debug "[$component]: pub: $msg ($(msg.data))"
    topic = msg.topic

    if (contains(topic, "/"))
        for space in keys(ksrouter.spaces)
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
                    for cmp in ksrouter.spaces[space]
                        @debug "[ksrouter] sending: $payload"
                        publish(cmp.second, cmp.first, topic, Rembus.msgdata(msg.data)...)
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

        if isa(msg, Rembus.AdminReqMsg) &&
           haskey(msg.data, Rembus.COMMAND) &&
           msg.data[Rembus.COMMAND] === Rembus.SUBSCRIBE_CMD
            @debug "[ksrouter][$(msg.twin)] subscribing: $msg"
            subscribe_handler(router, msg)
        elseif isa(msg, Rembus.PubSubMsg)
            publish_interceptor(router, msg)
        end

        # route to downstream broker
        put!(router.downstream.process.inbox, msg)
    end
end
