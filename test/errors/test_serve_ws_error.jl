include("../utils.jl")

# tests: 1

function run()
    try
        # invalid ssl configuration prevent ws_serve process startup
        caronte(wait=false, args=Dict("secure" => true))

        p = from("$BROKER_NAME.serve_ws")
        while p !== nothing
            @info "$p status=$(p.status)"
            sleep(0.5)
            p = from("$BROKER_NAME.serve_ws")
        end
        @test from("$BROKER_NAME.serve_ws") === nothing
    finally
        shutdown()
    end
end

@info "[test_serve_ws_error] start"
run()
@info "[test_serve_ws_error] stop"
