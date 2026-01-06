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

ENV["REMBUS_DIR"] = joinpath(tempdir(), "rembus")
ENV["REMBUS_TIMEOUT"] = "20"

"""
    request_timeout()

Return the default request timeout.
"""
function request_timeout()
    return parse(Float64, get(ENV, "REMBUS_TIMEOUT", "5"))
end

"""
    request_timeout!(value::Real)

Set the default request timeout used when creating new nodes with the
[`broker`](@ref), [`component`](@ref), [`connect`](@ref), or [`server`](@ref) functions.
"""
function request_timeout!(value::Real)
    ENV["REMBUS_TIMEOUT"] = string(value)
end

function challenge_timeout!(value)
    ENV["REMBUS_CHALLENGE_TIMEOUT"] = string(value)

end

function ack_timeout!(value)
    ENV["REMBUS_ACK_TIMEOUT"] = string(value)
end

function ws_ping_interval!(value)
    ENV["REMBUS_WS_PING_INTERVAL"] = string(value)
end

function zmq_ping_interval!(value)
    ENV["REMBUS_ZMQ_PING_INTERVAL"] = string(value)
end

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
        showerror(stdout, e, catch_backtrace())
        @test false
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


function init_ducklake(; reset=true)
    if haskey(ENV, "DUCKLAKE_URL")
        if startswith(ENV["DUCKLAKE_URL"], "ducklake:postgres")
            dbname = "rembus_test"
            user = get(ENV, "PGUSER", "postgres")
            pwd = get(ENV, "PGPASSWORD", "postgres")
            db_url = "postgresql://$user:$pwd@127.0.0.1/$dbname"
            ENV["DATABASE_URL"] = db_url
            ENV["DUCKLAKE_URL"] = "ducklake:postgres:$db_url"
            if reset
                Base.run(`dropdb $dbname --if-exists`)
                Base.run(`createdb $dbname`)
            end
        elseif startswith(ENV["DUCKLAKE_URL"], "ducklake:sqlite")
            db_file = joinpath(Rembus.rembus_dir(), "rembus_test.sqlite")
            if reset
                rm(db_file; force=true)
            end
            ENV["DUCKLAKE_URL"] = "ducklake:sqlite:$db_file"
        end
    elseif reset
        rm(joinpath(Rembus.rembus_dir(), "broker.ducklake"); force=true, recursive=true)
        rm(Rembus.broker_dir("broker"); force=true, recursive=true)
    end
end
