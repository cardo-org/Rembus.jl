using DataFrames
using JSONTables
using JSON3
using HTTP
using Logging
using Rembus
using Test

mutable struct Proxy <: Rembus.AbstractRouter
    downstream::Union{Nothing,Rembus.AbstractRouter}
    upstream::Union{Nothing,Rembus.AbstractRouter}
    process::Visor.Process
    function Proxy()
        return new(nothing, nothing)
    end
end

Rembus.rembus_dir!(joinpath(tempdir(), "rembus"))
Rembus.request_timeout!(20)

atexit() do
    fn = joinpath(@__DIR__, "..", "LocalPreferences.toml")
    rm(fn, force=true)
end

# default ca certificate name
const REMBUS_CA = "rembus-ca.crt"

function proxy_task(self, router)
    for msg in self.inbox
        #@debug "[proxy] recv: $msg"
        !isshutdown(msg) || break

        # route to downstream broker
        put!(router.downstream.process.inbox, msg)
    end
end

function start_proxy(supervisor_name, downstream_router)
    proxy = Proxy()
    Rembus.upstream!(downstream_router, proxy)
    sv = from(supervisor_name)
    proxy.process = process("proxy", proxy_task, args=(proxy,))
    startup(sv, proxy.process)
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

function run_broker(init, secure, ws, tcp, zmq, http, name, reset, authenticated)
    router = nothing
    if get(ENV, "BROKER_RUNNING", "0") == "0"
        if reset
            Rembus.broker_reset(name)
        end
        if init !== nothing
            init()
        end

        router = Rembus.get_router(
            secure=secure,
            ws=ws,
            tcp=tcp,
            zmq=zmq,
            http=http,
            name=name,
            authenticated=authenticated,
        )

        start_proxy(name, router)
    end

    return router
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
    router = run_broker(setup, secure, ws, tcp, zmq, http, testname, reset, authenticated)

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
