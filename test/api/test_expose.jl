include("../utils.jl")

myservice() = "ok"


function run()
    bro = broker(name="expose", ws=8338, zmq=8336, prometheus=7071) # ws=8338, tcp=8337, zmq=8336
    sleep(0.5)
    exposer = component("expose_exposer", ws=9000)
    client = broker(name="expose_client", ws=9900)

    expose(exposer, myservice)
    for url in ["ws://127.0.0.1:8338/expose_c1", "zmq://127.0.0.1:8336/expose_c1"]
        rb = connect(url)
        response = rpc(rb, "version")
        @info "response=$response"
        @test response == Rembus.VERSION

        response = rpc(rb, "myservice")
        @info "response=$response"
        @test response == "ok"

        close(rb)
    end

    close(exposer)
    close(client)
    close(bro)

end

@info "[expose] start"
try
    run()
catch e
    @test false
    @error "[expose] error: $e"
finally
    shutdown()
end
@info "[expose] stop"
