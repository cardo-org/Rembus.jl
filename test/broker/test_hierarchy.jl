include("../utils.jl")


mutable struct Zenoh <: Rembus.AbstractRouter
    # r"house/.*/temperature" => ["house/*/temperature" => twin]
    spaces::Dict{Regex,Vector{Pair{String,Rembus.Twin}}}
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function Zenoh()
        zenoh = new(Dict(), nothing, nothing)
        zenoh.process = process("zenoh", zenoh_task, args=(zenoh,))
        return zenoh
    end
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
                        publish(cmp.second, cmp.first, topic, Rembus.msgdata(msg.data)...)
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

function environment(topic, value; ctx, node)
    ctx[topic] = value
end

function run()
    ctx = Dict()

    temperature = 18.5
    wind = 3

    bro = broker(name="hierarchy")
    Rembus.add_plugin(bro, Zenoh())

    sub = component("hierarchy_sub")
    inject(sub, ctx)
    subscribe(sub, "veneto/*/cencenighe/*", environment)
    reactive(sub)

    node_name = "veneto/belluno/cencenighe"
    pub = component(node_name)
    put(pub, "temperature", temperature)
    put(pub, "wind", wind)

    sleep(1)
    close(sub)
    close(pub)
    close(bro)

    @test ctx[node_name*"/temperature"] == temperature
    @test ctx[node_name*"/wind"] == wind
end

@info "[hierarchy] start"
try
    request_timeout!(5)
    run()
catch e
    @test false
    @error "[hierarchy] error: $e"
finally
    shutdown()
end
@info "[hierarchy] stop"
