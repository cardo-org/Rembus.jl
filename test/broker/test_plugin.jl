include("../utils.jl")

test_name = "test_plugin"

# set a mismatched shared secret
function init(ok_cid, ko_cid)
    mkpath(Rembus.keys_dir(BROKER_NAME))
    # component side
    for cid in [ok_cid, ko_cid]
        pkfile = Rembus.pkfile(cid)
        open(pkfile, create=true, write=true) do f
            write(f, "aaa")
        end
    end

    # broker side
    fn = Rembus.key_base(BROKER_NAME, ok_cid)
    open(fn, create=true, write=true) do f
        write(f, "aaa")
    end
    fn = Rembus.key_base(BROKER_NAME, ko_cid)
    open(fn, create=true, write=true) do f
        write(f, "bbb")
    end

    set_admin(ok_cid)
end


module CarontePlugin

using Rembus

export challenge
export login

export myfunction

function myfunction(ctx, twin, arg)
    @info "myfunction: $(session(twin)) - isauth: $(twin.isauth)"
    return "hello from broker plugin"
end

function myfunction(ctx, twin, arg1, arg2)
    error("myfunction error")
end

function myfunction(ctx, twin)
    @info "myfunction: $(session(twin)) - isauth: $(twin.isauth)"
    return "no args"
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

function subscribe_handler(ctx, router, twin, msg)
    @info "[subscribe][$ctx]: $msg"
    ctx["subscribe"] = true

    # an exception generate an error log
    error("something wrong!")
end

function park(ctx, twin, msg)
    @info "[$twin] park: $msg"
    # an exception generate an error log
    error("parking error!")
end

function unpark(ctx, twin)
    @info "[$twin] upark"
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

function start_broker(ctx)
    rb = broker(
        wait=false,
        plugin=CarontePlugin,
        context=ctx,
        name=BROKER_NAME
    )
    forever(rb)
end

function run(ok_cid, ko_cid)
    # wait for secret files creation
    sleep(1)

    # store test related info
    ctx = Dict()

    Rembus.setup(Rembus.CONFIG)

    @async start_broker(ctx)
    sleep(2)

    rb = tryconnect(ok_cid)

    subscribe(rb, test_plugin_topic)

    # invoke myfunction defined by CarontePlugin module
    response = rpc(rb, "myfunction", "arg_1")
    @test response == "hello from broker plugin"

    response = rpc(rb, "myfunction", ["arg_1"])
    @test response == "hello from broker plugin"

    response = rpc(rb, "myfunction", nothing)
    @test response == "no args"

    try
        rpc(rb, "myfunction", ["arg_1", "arg_2"])
    catch e
        @info "expected error: $e"
        @test true
    end

    okcid = from("$BROKER_NAME.twins.ok_cid")
    twin = okcid.args[1]
    tim = Timer(0)
    msgid = 1
    twin.acktimer[1] = Rembus.AckState(false, tim)

    # triggers CarontePlugin.park
    Rembus.handle_ack_timeout(tim, twin, "my_string", msgid)

    # request a broker shutdown
    res = Rembus.broker_shutdown(rb)
    @info "shutdown: $res"

    close(rb)

    try
        rb = connect(ko_cid)
        @test false
    catch e
        @info "[$ko_cid] expected error: $e"
        @test isa(e, HTTP.Exceptions.ConnectError)
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
    remove_keys(ok_cid)
    remove_keys(ko_cid)
    shutdown()
    rm(Rembus.broker_dir(BROKER_NAME), recursive=true)
end
