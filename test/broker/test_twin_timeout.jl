include("../utils.jl")

function myservice()
    sleep(10)
    return 100
end


function run()
    curr_timeout = ENV["REMBUS_TIMEOUT"] = "0.5"
    ENV["REMBUS_TIMEOUT"] = "0.5"
    bro = component(name="bro1")

    @info "[twin_timeout] request_timeout: $(bro.router.settings.request_timeout)"

    srv = component("srv")
    expose(srv, myservice)

    cli = component("cli")


    try
        rpc(cli, "myservice")
        @test false
    catch e
        @info "[twin_timeout] expected error: $e"
        @test true
    end
    ENV["REMBUS_TIMEOUT"] = curr_timeout
end

@info "[twin_timeout] start"
try
    run()
catch e
    @error "[twin_timeout]: $e"
    @test false
finally
    shutdown()
end
@info "[twin_timeout] stop"
