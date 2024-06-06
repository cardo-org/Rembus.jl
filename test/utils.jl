using JSON3
using HTTP
using Logging
using Rembus
using Visor
using Test

const BROKER_NAME = "caronte_test"

results = []

macro start_caronte(init, args, reset)
    quote
        running = get(ENV, "CARONTE_RUNNING", "0") !== "0"
        params = $(esc(args))
        if !running
            name = get(params, "broker", BROKER_NAME)
            $(esc(reset)) && Rembus.caronte_reset(name)
            fn = $(esc(init))
            if fn !== nothing
                fn()
            end

            Rembus.caronte(wait=false, args=params)
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
        pth = joinpath(@__DIR__, "..", "..", "bin", "caronte")
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
    reset=true,
    setup=nothing,
    args=Dict("broker" => BROKER_NAME, "ws" => 8000, "tcp" => 8001, "zmq" => 8002)
)
    Rembus.setup(Rembus.CONFIG)
    @start_caronte setup args reset
    sleep(0.5)
    Rembus.logging()
    @info "[$testname] start"
    try
        fn()
    catch e
        @error "[$testname] failed: $e"
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
    for fn in [Rembus.pkfile(cid), Rembus.key_file(BROKER_NAME, cid)]
        @info "removing $fn"
        rm(fn, force=true)
    end
end
