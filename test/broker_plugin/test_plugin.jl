include("../utils.jl")

test_name = "test_plugin"

module CarontePlugin

export add_interest

function add_interest(ctx, router, twin, msg)
    @info "[add_interest][$ctx]: $msg"
    ctx["add_interest"] = true
end

function park(ctx, twin, msg)
    @info "[$twin] park: $msg"
end

function unpark(ctx, twin, msg)
    @info "[$twin] park: $msg"
end

#
# Called when the twin startup.
#
function twin_initialize(ctx, twin)
end

#
# Called back before the twin destruction.
#
function twin_finalize(ctx, twin)
end


end

function test_plugin_topic()
end

function run()
    # store test related info
    ctx = Dict()

    Rembus.set_broker_plugin(CarontePlugin)
    Rembus.set_broker_context(ctx)

    Rembus.setup(Rembus.CONFIG)

    topics = names(Rembus.CONFIG.broker_plugin)

    #exposed = filter(sym -> sym !== Symbol(Rembus.CONFIG.broker_plugin), topics)
    exposed = filter(
        sym -> isa(sym, Function), [getfield(Rembus.CONFIG.broker_plugin, t) for t in topics]
    )

    @info "topics =  $topics"
    @info "exposed =  $exposed"
    @info "isdefined(CONFIG.broker_plugin, :park) = $(isdefined(Rembus.CONFIG.broker_plugin, :park))"

    caronte(wait=false, exit_when_done=false)
    sleep(2)

    rb = connect()

    subscribe(rb, test_plugin_topic)

    sleep(3)

    close(rb)

    @test ctx["add_interest"]
end


try
    run()
catch e
    @error "[$test_name]: $e"
    @test false
finally
    shutdown()
end
