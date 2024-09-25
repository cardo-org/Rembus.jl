include("../utils.jl")

# tests: 1

function run()
    try

        @test_throws Rembus.CABundleNotFound Rembus.rembus_ca()

        # invalid ssl configuration prevent ws_serve process startup
        caronte(wait=false, args=Dict("name" => BROKER_NAME, "secure" => true))

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
# Force a ssl configuration error when starting the broker
current_dir = Rembus.rembus_dir!("/tmp/rembus")
run()
Rembus.rembus_dir!(current_dir)
@info "[test_serve_ws_error] stop"
