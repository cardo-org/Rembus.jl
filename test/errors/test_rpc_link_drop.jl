include("../utils.jl")

# S1: the caller's link drops while its RPC is in flight. The request must
# complete with RembusTimeout (not wait forever) and a later RPC must succeed
# after the automatic reconnection.

function slow(x)
    sleep(5)
    return x
end

fast(x) = x

function run()
    exposer = connect("link_drop_exposer")
    expose(exposer, slow)
    expose(exposer, fast)

    caller = component("link_drop_caller")
    Rembus.request_timeout!(caller, 2)

    t0 = time()
    task = @async try
        rpc(caller, "slow", 1)
    catch e
        e
    end
    sleep(0.5)
    # Drop the caller's own link while the request is pending.
    caller.socket.sock.close_transport!()

    status = timedwait(() -> istaskdone(task), 10.0)
    @test status == :ok
    if status == :ok
        @test fetch(task) isa RembusTimeout
        @test time() - t0 < 6
    end

    # The caller reconnects automatically and can issue new requests.
    result = nothing
    for _ in 1:20
        try
            result = rpc(caller, "fast", 2)
            break
        catch
            sleep(0.5)
        end
    end
    @test result == 2

    close(caller)
    close(exposer)
end

execute(run, "rpc_link_drop")
