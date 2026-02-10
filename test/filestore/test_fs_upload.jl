include("../utils.jl")

myservice1() = "ok"
myservice2() = "ok"

mytopic() = nothing

function run()
    bro = broker(name="fs_upload", ws=8000)
    Rembus.islistening(bro, protocol=[:ws], wait=10)
    srv1 = component("srv1")
    srv2 = component("srv2")
    client = component("cli")

    expose(srv1, myservice1)
    expose(srv2, myservice1)
    expose(srv2, myservice2)
    subscribe(srv1, mytopic)
    subscribe(srv2, mytopic)

    close(srv1)
    close(srv2)
    close(client)
    close(bro)

    # a filestore backend is always opened.
    @test isopen(Rembus.FileStore())

end

function restart()
    @info "restarting ..."
    bro = broker(name="fs_upload", ws=8000, zmq=8002, prometheus=7071)
    Rembus.islistening(bro, protocol=[:ws], wait=10)
    srv1 = component("srv1")
    srv2 = component("srv2")
    sleep(1)
    close(srv1)
    close(srv2)

end

@info "[fs_upload] start"
try
    run()
    restart()
catch e
    @test false
    @error "[fs_upload] error: $e"
finally
    shutdown()
end
@info "[fs_upload] stop"
