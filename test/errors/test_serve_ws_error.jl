include("../utils.jl")


function run()
    # invalid ssl configuration prevent ws_serve process startup
    caronte(wait=false, args=Dict("secure" => true))
    sleep(2)
    @test from("caronte.serve_ws") === nothing
end

@info "[test_serve_ws_error] start"
run()
@info "[test_serve_ws_error] stop"
