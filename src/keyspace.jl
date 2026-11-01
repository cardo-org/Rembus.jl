using Rembus

# Implements a Key Expressions Language similar to zenoh:
# https://github.com/eclipse-zenoh/roadmap/blob/main/rfcs/ALL/Key%20Expressions.md

struct SpaceTwin
    space::String
    twin::Twin
end

Base.:(==)(a::SpaceTwin, b::SpaceTwin) =
    a.space == b.space && a.twin == b.twin

Base.hash(st::SpaceTwin, h::UInt) = hash((st.space, st.twin), h)

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

#=
    has_keyspace(router)

Return `true` if a `KeySpaceRouter` (the wildcard extension) is part of the
router chain.
=#
function has_keyspace(router)
    r = bottom_router(router)
    while !isnothing(r)
        isa(r, KeySpaceRouter) && return true
        r = r.downstream
    end
    return false
end

#=
    valid_pattern(topic)

Return `true` if `topic` is a valid wildcard pattern: `*` and `**` are valid
only as a whole `/`-separated segment.
=#
function valid_pattern(topic::AbstractString)
    return all(split(topic, "/")) do seg
        !contains(seg, "*") || seg == "*" || seg == "**"
    end
end

# Escape the regex metacharacters of a literal pattern segment.
escape_literal(seg) = replace(seg, r"[\\.^$|?+()\[\]{}]" => s"\\\0")

#=
    build_space_re(topic)

Regular expression of a wildcard pattern: `*` matches one non-empty segment,
`**` matches any (possibly empty) character sequence, every other character
matches itself.
=#
function build_space_re(topic)
    segments = map(split(topic, "/")) do seg
        seg == "**" ? ".*" : seg == "*" ? "[^/]+" : escape_literal(seg)
    end
    return Regex("^" * join(segments, "/") * "\$")
end

# The regex generated for the pattern that matches every possible topic.
const GLOB_STAR_PATTERN = "^.*\$"

#=
    is_glob_star(topic)

Return `true` if `topic` is the wildcard pattern `**` that matches every
topic, as opposed to a per-segment wildcard pattern
(e.g. `veneto/*/cencenighe/*`) used for hierarchical key-expression routing.
=#
is_glob_star(topic::AbstractString) = build_space_re(topic).pattern == GLOB_STAR_PATTERN

#=
    mark_glob_topic!(router, topic)

Track `topic` in `router.glob_topics` when it is a "match everything" glob
pattern, so that broadcasting to "**"/`/**/` subscribers does not need to
evaluate glob patterns against each published topic.
=#
function mark_glob_topic!(router, topic::AbstractString)
    if is_glob_star(topic)
        push!(router.glob_topics, topic)
    end

    return nothing
end

#=
    unmark_glob_topic!(router, topic)

Remove `topic` from `router.glob_topics` once it has no more subscribers.
=#
function unmark_glob_topic!(router, topic::AbstractString)
    if !haskey(router.topic_interests, topic)
        delete!(router.glob_topics, topic)
    end

    return nothing
end

function setup_twin(router::KeySpaceRouter, twin::Twin)
    @debug "[ksrouter] topic_interests: $(router.downstream.topic_interests)"
    for topic in keys(router.downstream.topic_interests)
        if twin in router.downstream.topic_interests[topic]
            @debug "[ksrouter] subscribing twin $twin to topic $topic"
            subscribe_handler(router, twin, topic)
        end
    end

    return nothing
end

function subscribe_handler(ksrouter, component, topic)
    @debug "[$component][subscribe] ksrouter:$ksrouter, topic:$topic"

    if contains(topic, "*") && valid_pattern(topic)
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
    # An invalid publish is dropped by the core router: never match it.
    invalid_publish_topic(top_router(ksrouter), topic) && return nothing

    for (space_re, space_twins) in ksrouter.spaces
        # The match-everything pattern is served by the core broker.
        space_re.pattern == GLOB_STAR_PATTERN && continue
        if occursin(space_re, topic)
            core = top_router(ksrouter)
            for st in space_twins
                tw = st.twin
                # An exact-topic subscription wins: one message per publish.
                tw in get(core.topic_interests, topic, ()) && continue
                # Same delivery conditions of an exact-topic subscription,
                # evaluated against the original topic.
                deliverable = isopen(tw) && tw !== component && tw.reactive &&
                              domain(tw) == domain(component) &&
                              isauthorized(core, tw, topic)
                if deliverable
                    @debug "[ksrouter] sending $msg to $tw"
                    data = Any[topic; Rembus.msgdata(msg.data)...]
                    out = PubSubMsg(tw, st.space, data, msg.flags & QOS2)
                    publish_msg(tw, out)
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
                subscribe_handler(router, msg.twin, msg.topic)
            elseif msg.data[Rembus.COMMAND] === Rembus.UNSUBSCRIBE_CMD
                @debug "[ksrouter][$(msg.twin)] unsubscribing: $msg"
                unsubscribe_handler(router, msg)
            end
        elseif isa(msg, Rembus.PubSubMsg)
            publish_interceptor(router, msg)
        end

        # route to downstream broker (in the direction of the top router)
        put!(router.downstream.process.inbox, msg)
    end
end
