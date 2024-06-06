include("../utils.jl")

# tests: 1

function run()
    # invalid ssl configuration prevent ws_serve process startup
    caronte(wait=false, args=Dict("secure" => true))

    p = from("caronte.serve_ws")
    while p !== nothing
        @info "$p status=$(p.status)"
        sleep(0.5)
        p = from("caronte.serve_ws")
    end
    @test from("caronte.serve_ws") === nothing
end

@info "[test_serve_ws_error] start"
run()
@info "[test_serve_ws_error] stop"
