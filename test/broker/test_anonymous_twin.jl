include("../utils.jl")

function run()
    component(ws=9000, name="bro1")

    bro2 = component(ws=8000, name="bro2")

    Rembus.settings(bro2).connection_mode = Rembus.authenticated

    # Throw "anonymous components not allowed"
    @test_throws ErrorException component("ws://:9000", name="bro2")
end

@info "[anonymous_twin] start"
try
    run()
catch e
    @error "[anonymous_twin]: $e"
    @test false
finally
    shutdown()
end
@info "[anonymous_twin] stop"
