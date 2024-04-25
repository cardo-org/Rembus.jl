using HTTP
using Logging
using Rembus
using Visor
using Test

results = []

macro start_caronte(init, args)
    quote
        running = get(ENV, "CARONTE_RUNNING", "0") !== "0"
        if !running
            caronte_reset()
            fn = $(esc(init))
            if fn !== nothing
                fn()
            end

            Rembus.caronte(wait=false, exit_when_done=false, args=$(esc(args)))
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
        p = Base.run(Cmd(`$(@__DIR__)/../bin/caronte`, detach=true), wait=false)
    end
    sleep(10)

    Rembus.logging(debug=[:test])
    @info "[$testname] start"
    try
        fn()
    finally
        shutdown()
        sleep(3)
        if !running
            Base.kill(p, Base.SIGINT)
        end
        sleep(3)
    end
    @info "[$testname] stop"
end

function execute(fn, testname; setup=nothing, args=Dict())
    Rembus.setup(Rembus.CONFIG)
    @start_caronte setup args
    sleep(0.5)
    Rembus.logging(debug=[:test])
    @info "[$testname] start"
    try
        fn()
    catch e
        @error e
        showerror(stdout, e, catch_backtrace())
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
    maxretries = 10
    count = 0
    while true
        try
            return connect(id)
        catch e
            if !isa(e, HTTP.Exceptions.ConnectError)
                @warn "error: $e"
            end
            count === maxretries && rethrow(e)
            count += 1
        end
        sleep(1)
    end
end

function caronte_reset()
    Rembus.CONFIG = Rembus.Settings()
    foreach(d -> rm(d, recursive=true), readdir(Rembus.twins_dir(), join=true))
    foreach(rm, filter(isfile, readdir(Rembus.CONFIG.db, join=true)))
end

function remove_keys(cid)
    for fn in [Rembus.pkfile(cid), Rembus.key_file(cid)]
        @info "removing $fn"
        rm(fn, force=true)
    end
end
