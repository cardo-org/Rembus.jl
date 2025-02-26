using DataFrames
using JSONTables
using JSON3
using HTTP
using Logging
using Rembus
using Test

Rembus.rembus_dir!("/tmp/rembus")
Rembus.request_timeout!(20)

# default ca certificate name
const REMBUS_CA = "rembus-ca.crt"

results = []

function modify_env(var, value)
    current_val = get(ENV, var, nothing)
    ENV[var] = value
    return current_val
end

"""
    check_sentinel(ctx; sentinel=missing, max_wait=10)

Wait until `ctx` has key sentinel or if sentinel is missing
until `ctx.count` is greater then zero.
"""
function check_sentinel(ctx; sentinel=missing, max_wait=10)
    wtime = 0.1
    t = 0
    sts = false
    while t < max_wait
        t += wtime
        sleep(wtime)
        if ismissing(sentinel)
            if ctx.count > 0
                sts = true
                break
            end
        else
            if haskey(ctx, sentinel)
                sts = true
                break
            end
        end
    end

    return sts
end

macro broker(init, secure, ws, tcp, zmq, http, name, reset, authenticated, log)
    quote
        running = get(ENV, "BROKER_RUNNING", "0") !== "0"
        if !running
            if $(esc(reset))
                Rembus.broker_reset($(esc(name)))
            end
            fn = $(esc(init))
            if fn !== nothing
                fn()
            end

            Rembus.start_broker(
                secure=$(esc(secure)),
                ws=$(esc(ws)),
                tcp=$(esc(tcp)),
                zmq=$(esc(zmq)),
                http=$(esc(http)),
                name=$(esc(name)),
                authenticated=$(esc(authenticated)),
            )
        end
    end
end

function execute(
    fn,
    testname;
    authenticated=false,
    reset=true,
    setup=nothing,
    check_listeners=[:ws],
    ws=8000,
    tcp=8001,
    zmq=8002,
    http=nothing,
    secure=false,
)
    router = @broker setup secure ws tcp zmq http testname reset authenticated log

    if !isnothing(router)
        Rembus.islistening(
            router,
            protocol=check_listeners,
            wait=20,
        )
    end
    @info "[$testname] start"
    try
        fn()
    catch e
        @error "[$testname] failed: $e"
        @test false
        showerror(stdout, e, catch_backtrace())
    finally
        shutdown()
    end
    @info "[$testname] stop"
end

# name become an admin
function set_admin(broker, name)
    broker_dir = joinpath(Rembus.rembus_dir(), broker)
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end

    # add admin privilege to client with name equals to test_private
    fn = joinpath(broker_dir, "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([name])))
    end
end

function remove_keys(broker_name, cid)
    for fn in [
        Rembus.pkfile(cid),
        Rembus.key_file(broker_name, cid),
    ]
        if fn !== nothing
            @info "removing $fn"
            rm(fn, force=true)
        end
    end
end
