include("../utils.jl")

test_name = "test_plugin"

# set a mismatched shared secret
function init(ok_cid, ko_cid)
    # component side
    for cid in [ok_cid, ko_cid]
        pkfile = Rembus.pkfile(cid)
        open(pkfile, create=true, write=true) do f
            write(f, "aaa")
        end
    end

    # broker side
    fn = Rembus.key_file(ok_cid)
    open(fn, create=true, write=true) do f
        write(f, "aaa")
    end

    fn = Rembus.key_file(ko_cid)
    open(fn, create=true, write=true) do f
        write(f, "bbb")
    end
end


module CarontePlugin

using Rembus #needed for session()

export add_interest
export challenge
export login
export myfunction

function myfunction(twin)
    @info "myfunction: $(session(twin)) - isauth: $(twin.isauth)"
    return "hello from caronte plugin"
end

function challenge(twin)
    return UInt8[0, 0, 0, 0]
end

function login(twin, user, hash)
    sess = session(twin)
    @info "[$twin] custom login [$user]: $sess"
    if user == "ok_cid"
        return true
    end
    return false
end

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

function run(ok_cid, ko_cid)
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

    rb = connect(ok_cid)

    subscribe(rb, test_plugin_topic)

    # invoke myfunction defined by CarontePlugin module
    response = rpc(rb, "myfunction")
    @test response == "hello from caronte plugin"
    close(rb)

    try
        rb = connect(ko_cid)
        @test false
    catch e
        @info "[$ko_cid] expected error: $e"
        @test isa(e, Rembus.RembusError)
    end

    sleep(1)

    @test ctx["add_interest"]
end

ok_cid = "ok_cid"
ko_cid = "ko_cid"

try
    init(ok_cid, ko_cid)
    run(ok_cid, ko_cid)
catch e
    @error "[$test_name]: $e"
    @test false
finally
    remove_keys(ok_cid)
    remove_keys(ko_cid)
    shutdown()
end