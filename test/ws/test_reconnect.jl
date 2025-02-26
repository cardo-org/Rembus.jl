include("../utils.jl")

function run()
    bro = broker(ws=8000)

    rb = component("reconnect_component")
    @test rpc(rb, "version") == Rembus.VERSION

    shutdown(bro)
    sleep(1)
    @test !isopen(rb)
    bro = broker(ws=8000)
    sleep(3)
    @test isopen(rb)

end

@info "[reconnect] start"
try
    run()
catch e
    @test false
    @error "[reconnect] error: $e"
finally
    shutdown()
end
@info "[reconnect] stop"
