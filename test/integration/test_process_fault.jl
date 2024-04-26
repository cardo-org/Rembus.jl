include("../utils.jl")

using Distributed
using Visor

restarts = 0

function trace(supervisor, msg)
    global restarts
    if isa(msg, Visor.ProcessError)
        restarts += 1
        if restarts == 2
            shutdown()
        end
    end
end

function run()
    # Component Under Test
    cid = "test_process"

    Rembus.caronte_reset()

    Visor.trace_event = trace
    supervise([rembus(cid)], intensity=3)
end

try
    @info "[test_process_fault] start"
    run()
    @debug "task restarts: $restarts" _group = :test
    @test 1 <= restarts <= 2
catch e
    @error "[test_process_fault]: $e"
    showerror(stdout, e, catch_backtrace())
finally
    @info "[test_process_fault] stop"
end
