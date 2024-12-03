include("../utils.jl")

using JSON3

const CID = "ws://127.0.0.1:9000/myserver"

function setup()
    dir = Rembus.broker_dir(BROKER_NAME)
    if !isdir(dir)
        mkpath(dir)
    end

    subscribers = Dict(CID => Dict())
    open(joinpath(dir, "subscribers.json"), "w") do f
        write(f, JSON3.write(subscribers))
    end
end

function teardown()
    dir = Rembus.broker_dir(BROKER_NAME)
    rm(joinpath(dir, "subscribers.json"), force=true)
    rm(joinpath(dir, "servers.json"), force=true)
end

myservice() = 100

function run()
    srv = server(ws=9000)
    sleep(1)
    bro = broker(wait=false, name=BROKER_NAME)
    add_node(bro, CID)

    sleep(0.1)
    rb = connect()
    expose(rb, myservice)
    result = rpc(srv, "myservice")
    @info "[test_broker_connect] result=$result"
    @test result == 100
    close(rb)
end

@info "[test_broker_connect] start"
try
    setup()
    run()
catch e
    @error "[test_broker_connect] unxpected error: $e"
    @test false
finally
    shutdown()
    teardown()
end
@info "[test_broker_connect] stop"
