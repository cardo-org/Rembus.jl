include("../utils.jl")

test_name = "test_plugin"
broker_name = "plugin"

# set a mismatched shared secret
function init(ok_cid, ko_cid)
    mkpath(Rembus.keys_dir(broker_name))
    # component side
    for cid in [ok_cid, ko_cid]
        pkfile = Rembus.pkfile(cid, create_dir=true)
        open(pkfile, create=true, write=true) do f
            write(f, "aaa")
        end
    end

    # broker side
    fn = Rembus.key_base(broker_name, ok_cid)
    open(fn, create=true, write=true) do f
        write(f, "aaa")
    end
    fn = Rembus.key_base(broker_name, ko_cid)
    open(fn, create=true, write=true) do f
        write(f, "bbb")
    end

    set_admin(broker_name, ok_cid)
end


module CarontePlugin

using Rembus

export challenge
export login

function challenge(twin)
    @info "challenge invoked"
    return UInt8[0, 0, 0, 0]
end

function login(twin, user, hash)
    @info "[$twin] custom login [$user]: $(twin.shared)"
    if user == "ok_cid"
        return true
    end
    return false
end

function subscribe_handler(ctx, router, twin, msg)
    @info "[subscribe][$ctx]: $msg"
    ctx["subscribe"] = true

    # an exception generate an error log
    error("something wrong!")
end

function park(msg; ctx, node)
    @info "[$node] park: $msg"
    # an exception generate an error log
    error("parking error!")
end

function unpark(; ctx, node)
    @info "[$node] upark"
end

function save_configuration(ctx, router)
    @info "CarontePlugin::save_configuration"
    error("save configuration failed")
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

end # module CarontePlugin

function test_plugin_topic()
end

function run(ok_cid, ko_cid)
    # store test related info
    ctx = Dict()

    bro = broker(name=broker_name, ws=8000)
    Rembus.set_plugin(bro, CarontePlugin, ctx)

    Rembus.islistening(bro, protocol=[:ws], wait=10)

    rb = connect(ok_cid)
    subscribe(rb, test_plugin_topic)
    close(rb)

    try
        rb = connect(ko_cid)
        @test false
    catch e
        @info "[$ko_cid] expected error: $e"
        @test isa(e, RembusError)
    end

    sleep(1)

    @test ctx["subscribe"]
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
    shutdown()
end
