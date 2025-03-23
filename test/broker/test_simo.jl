include("../utils.jl")

#=
    ha_setup_c1 -- ha_setup_main
                       |
                       |
                ha_setup_failover
=#

Rembus.info!()

broker_name = "simo_broker"
server_name = "simo_server"

fixture = Dict(
    "a" => 1,
    "b" => 2,
    "c" => missing,
    "d" => 4
)

function myservice(ctx, rb, source, val)
    if source === "c"
        sleep(4)
    end
    return fixture[source]
end

mutable struct SiMo <: Rembus.AbstractRouter
    registry::Dict # source_message_id => [futures]
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function SiMo()
        obj = new(Dict(), nothing, nothing)
        obj.process = process("simo", simo_task, args=(obj,))
        return obj
    end
end

function wait_responses(simo, msg, fut_responses)
    try
        results = []
        for (name, fut_res) in fut_responses
            result = fetch(fut_res.future)
            if isa(result, RembusTimeout)
                push!(results, (name, missing))
            else
                push!(results, (name, Rembus.msgdata(result.data)))
            end
        end
        put!(
            simo.inbox,
            Rembus.ResMsg(msg.twin, msg.id, Rembus.STS_SUCCESS, results)
        )
    catch e
        @error "[wait_responses] error: $e"
    end
end

function simo_task(self, router)
    for msg in self.inbox
        @debug "[simo] recv: $msg"
        !isshutdown(msg) || break

        if isa(msg, Rembus.RpcReqMsg)
            requests = []
            for tw in Rembus.alltwins(router)
                push!(requests, (rid(tw), Rembus.fpc(
                    tw, msg.topic,
                    (rid(tw), msg.data...),
                    Rembus.request_timeout() - 1
                )))
            end
            @async wait_responses(self, msg, requests)
        elseif isa(msg, Rembus.ResMsg)
            @debug "[simo] result: $msg $(Rembus.msgdata(msg.data))"
            put!(router.downstream.process.inbox, msg)
        else
            put!(router.downstream.process.inbox, msg)
        end
    end
end

function start_broker(broker_name)
    return broker(name=broker_name)
end

function run()
    ctx = Dict()
    pool = ["a", "b", "c", "d"]

    bro = start_broker(broker_name)
    rb = component(pool, name="simo_pool")
    Rembus.add_plugin(rb, SiMo())

    srv = component(server_name, ws=8001)
    expose(srv, myservice)
    inject(srv, ctx)

    results = rpc(rb, "myservice", 1)
    @test length(results) === length(pool)
    for (name, result) in results
        @info "[ha_setup] $name => $result"
        if name === "c"
            @test ismissing(result)
        else
            @test result == fixture[name]
        end
    end

    shutdown()
end

@info "[ha_setup] start"
try
    Rembus.request_timeout!(5)
    run()
catch e
    @test false
    @error "[ha_setup] error: $e"
finally
    shutdown()
end
@info "[ha_setup] stop"
