using DataFrames
using JSONTables
using JSON3
using HTTP
using Logging
using Rembus
using Test

Rembus.rembus_dir!("/tmp/rembus")

const BROKER_NAME = "caronte_test"
const SERVER_NAME = "server_test"

# default ca certificate name
const REMBUS_CA = "rembus-ca.crt"

results = []

"""
    check_sentinel(ctx; max_wait=10)

Wait until `ctx.count` is greater then zero and return `true`.
Return `false` if `max_wait` seconds have passed and `ctx.count` is not greater than zero.
"""
function check_sentinel(ctx; max_wait=10)
    wtime = 0.1
    t = 0
    while t < max_wait
        t += wtime
        sleep(wtime)
        if ctx.count > 0
            break
        end
    end
end

macro start_caronte(init, secure, policy, ws, tcp, zmq, http, name, reset, mode, log)
    quote
        running = get(ENV, "CARONTE_RUNNING", "0") !== "0"
        if !running
            if $(esc(reset))
                Rembus.caronte_reset($(esc(name)))
            end
            fn = $(esc(init))
            if fn !== nothing
                fn()
            end

            Rembus.broker(
                wait=false,
                secure=$(esc(secure)),
                policy=$(esc(policy)),
                ws=$(esc(ws)),
                tcp=$(esc(tcp)),
                zmq=$(esc(zmq)),
                http=$(esc(http)),
                name=$(esc(name)),
                mode=$(esc(mode)),
                log=$(esc(log))
            )
        end
    end
end


macro atest(expr, descr=nothing)
    if descr === nothing
        descr = string(expr)
    end
    :(push!(results, $(esc(descr)) => $(esc(expr))))
end

function execute_caronte_process(fn, testname; setup=nothing)
    running = get(ENV, "CARONTE_RUNNING", "0") !== "0"

    if !running
        pth = joinpath(@__DIR__, "..", "..", "bin", "broker")
        p = Base.run(Cmd(`$pth`, detach=true), wait=false)
    end
    sleep(10)

    Rembus.logging()
    @info "[$testname] start"
    try
        fn()
    finally
        shutdown()
        sleep(2)
    end
    if !running
        Base.kill(p, Base.SIGINT)
    end
    sleep(2)
    @info "[$testname] stop"
end

function execute(
    fn,
    testname;
    mode="anonymous",
    reset=true,
    setup=nothing,
    islistening=["serve_ws"],
    ws=8000,
    tcp=8001,
    zmq=8002,
    http=nothing,
    secure=false,
    policy=:first_up,
    log="info"
)
    Rembus.setup(Rembus.CONFIG)

    @start_caronte setup secure policy ws tcp zmq http BROKER_NAME reset mode log
    sleep(0.5)
    procs = ["$BROKER_NAME.$proc" for proc in islistening]
    Rembus.islistening(
        wait=20,
        procs=procs
    )
    Rembus.logging()
    @info "[$testname] start"
    try
        fn()
    catch e
        @error "[$testname] failed: $e"
        @test false
        #showerror(stdout, e, catch_backtrace())
    finally
        shutdown()
    end
    @info "[$testname] stop"
end

function testsummary()
    global results
    for (descr, t) in results
        @debug "$descr: $(t ? "pass" : "fail")"
        @test t
    end
    empty!(results)
end

function tryconnect(id)
    maxretries = 5
    count = 0
    while true
        try
            return Rembus.connect(id)
        catch e
            if !isa(e, HTTP.Exceptions.ConnectError)
                @warn "tryconnect: $e"
                showerror(stdout, e, catch_backtrace())
            end
            count === maxretries && rethrow(e)
            count += 1
        end
        sleep(2)
    end
end

# name become an admin
function set_admin(name)
    if !isdir(Rembus.broker_dir(BROKER_NAME))
        mkdir(Rembus.broker_dir(BROKER_NAME))
    end

    # add admin privilege to client with name equals to test_private
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([name])))
    end
end

function remove_keys(cid)
    for fn in [
        Rembus.pkfile(cid),
        Rembus.key_file(BROKER_NAME, cid),
        Rembus.key_file(SERVER_NAME, cid)
    ]
        if fn !== nothing
            @info "removing $fn"
            rm(fn, force=true)
        end
    end
end

function verify_counters(; total::Int, components::Dict)
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "twins.json")
    if isfile(fn)
        content = read(fn, String)
        twinid_mark = JSON3.read(content, Dict{String,UInt64})
        counter = pop!(twinid_mark, "__counter__")
        @test counter == total
        for (id, expected_mark) in components
            @test Int(twinid_mark[id]) == expected_mark
        end
    end
end
