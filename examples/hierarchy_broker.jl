using Rembus

# Implements a Key Expressions Language similar to zenoh:
# https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md

mutable struct Ctx
    # r"house/.*/temperature" => ["house/*/temperature" => twin]
    spaces::Dict{Regex,Vector{Pair{String,Rembus.Twin}}}
    Ctx() = new(Dict())
end

module CarontePlugin

using Rembus

function subscribe_handler(ctx, broker, component, msg)
    topic = msg.topic
    @debug "[$component][subscribe_handler] ctx:$ctx, topic:$topic"

    if contains(topic, "*")
        str = replace(topic, "/**/" => "(.*)", "**" => "(.*)", "*" => "([^/]+)")
        retopic = Regex("^$str\$")
        if haskey(ctx.spaces, retopic)
            push!(ctx.spaces[retopic], Pair(topic, component))
        else
            ctx.spaces[retopic] = [Pair(topic, component)]
        end
    end
end

function publish_interceptor(ctx, component, msg)
    @debug "[$component]: pub: $msg ($(msg.data))"
    topic = msg.topic

    if (contains(topic, "/"))
        payload = msg_payload(msg.data)
        if isa(payload, Vector)
            payload = Vector{Any}(payload)
            pushfirst!(payload, topic)
        else
            payload = [topic, payload]
        end

        for space in keys(ctx.spaces)
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
                    for cmp in ctx.spaces[space]
                        @info "sending: $payload"
                        publish(cmp.second, cmp.first, payload)
                    end
                end
            end
        end

    end

    # broadcast the original message
    return true
end

end

caronte(plugin=CarontePlugin, context=Ctx())
