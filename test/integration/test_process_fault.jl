include("../utils.jl")

using Distributed
using Visor

restarts = 0

function trace(supervisor, msg)
    global restarts
    if isa(msg, Visor.ProcessError)
        restarts += 1
    end
end

function run()
    # Component Under Test
    cid = "test_process"

    caronte_reset()

    Visor.trace_event = trace

    @async supervise([rembus(cid)], intensity=3)
    sleep(3)

    remotecall(Rembus.caronte, 2, exit_when_done=false)
    sleep(2)
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
    shutdown()
    remotecall(Visor.shutdown, 2)
    sleep(3)
    @info "[test_process_fault] stop"
end