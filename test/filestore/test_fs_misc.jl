include("../utils.jl")
include("../../tools/tenant.jl")

using DataFrames
using TestItems

function test_save_acks_file()
    comp = "component1"
    bro = "test_router"
    Rembus.broker_reset(bro)
    dir = joinpath(Rembus.rembus_dir(), bro, comp)
    @info "creating dir $dir"
    mkpath(dir)

    router = Rembus.Router{Rembus.Twin}(bro)
    router.con = Rembus.FileStore()
    Rembus.load_configuration(router)
    fn = Rembus.acks_file(router, comp)
    if isfile(fn)
        rm(fn)
    end

    url = Rembus.RbURL(comp)
    twin = Rembus.Twin(url, router)
    msg = Rembus.PubSubMsg(twin, "topic", "data", Rembus.QOS2)
    twin.ackdf = Rembus.load_received_acks(router, url, Rembus.top_router(router).con)
    Rembus.add_pubsub_id(twin, msg)
    Rembus.save_received_acks(twin, Rembus.top_router(router).con)

    # reload the acks file
    twin2 = Rembus.Twin(url, router)
    twin2.ackdf = Rembus.load_received_acks(router, url, Rembus.top_router(router).con)
    @test nrow(twin2.ackdf) == 1
    @test twin.ackdf[1, :] == twin2.ackdf[1, :]
end

function test_get_router()
    name = "myrouter"
    add_tenant(".", "mysecret", name)

    r1 = Rembus.get_router(name=name, ws=8000, tcp=8001, zmq=8002, prometheus=9000)
    r2 = Rembus.get_router(name=name, ws=8000, tcp=8001, zmq=8002, prometheus=9000)
    @test r1 === r2

    Rembus.uptime(r1)
    shutdown()
end

function test_load_messages()
    name = "myrouter"
    #router = Rembus.Router{Rembus.Twin}(name)
    bro = component(name=name)

    # create a synthetic message file
    mdir = Rembus.messages_dir(name)
    touch(joinpath(mdir, "1000"))
    sleep(0.1)

    #twin = Rembus.Twin(Rembus.RbURL("mytwin"), router)
    mytwin = component("mytwin")
    sleep(1)
    Visor.dump()
    bro_twin = from("myrouter.twins.mytwin").args[1]
    Rembus.messages_files(bro_twin, 0)

    close(bro_twin)
    close(bro)
end


function test_load_topic_auth()
    router = Rembus.get_router(name="myrouter", http=6754)
    cfg = Dict("mytopic" => Dict("mycid" => true))
    router.topic_auth = cfg
    Rembus.save_topic_auth(router, router.con)
    router.topic_auth = Dict()
    Rembus.load_topic_auth(router, router.con)
    @test router.topic_auth == cfg
    shutdown()
end

test_load_topic_auth()
test_get_router()
test_save_acks_file()
test_load_messages()
