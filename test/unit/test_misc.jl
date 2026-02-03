include("../utils.jl")

using DataFrames
using TestItems

@testitem "float_socket" begin
    router = Rembus.Router{Rembus.Twin}("test_router")
    twin = Rembus.Twin(Rembus.RbURL("component1"), router)
    twin2 = Rembus.Twin(Rembus.RbURL("component2"), router)
    msg = Rembus.RpcReqMsg(twin, "test_topic", "data")

    msg2 = Rembus.copy_from(msg) do msg
        msg.twin = twin2
    end

    @test msg2.topic === "test_topic"
    @test msg2.twin === twin2

    @test !isopen(twin.socket)
    @test wait(twin, reactive=false) === nothing
    @test !Rembus.isconnectionerror(twin.socket, ErrorException("something nasty"))
end

@testitem "cleanup" begin
    router = Rembus.Router{Rembus.Twin}("test_router")
    twin = Rembus.Twin(Rembus.RbURL("component1"), router)

    router.topic_impls["myservice"] = Set([twin])
    router.topic_interests["mytopic"] = Set([twin])

    Rembus.cleanup(twin, router)
    @test isempty(router.topic_impls)
    @test isempty(router.topic_interests)
end

@testitem "save_acks_file" begin
    using DataFrames

    comp = "component1"
    bro = "test_router"
    Rembus.broker_reset(bro)
    dir = joinpath(Rembus.rembus_dir(), bro, comp)
    @info "creating dir $dir"
    mkpath(dir)

    router = Rembus.Router{Rembus.Twin}(bro)
    Rembus.boot(router)
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

@testitem "get_router" begin
    r1 = Rembus.get_router(name="myrouter", ws=8000, tcp=8001, zmq=8002, prometheus=9000)
    r2 = Rembus.get_router(name="myrouter", ws=8000, tcp=8001, zmq=8002, prometheus=9000)
    @test r1 === r2

    Rembus.uptime(r1)
    shutdown()
end

@testitem "default_broker" begin
    ENV["REMBUS_DIR"] = joinpath(tempdir(), "rembus")
    rb = broker()
    @test Rembus.islistening(rb, wait=10)

    request_timeout!(rb, 20)
    @test request_timeout(rb) == 20
    shutdown()
end

@testitem "default_server" begin
    ENV["REMBUS_DIR"] = joinpath(tempdir(), "rembus")
    rb = server()
    @test Rembus.islistening(rb, wait=10)
    shutdown()
end

@testitem "reconnect" begin
    rb = component("misc_component")
    # Give opportunity to retry, the reconnection period is 2 seconds.
    sleep(3)
    @test !isopen(rb)
    shutdown()
end

@testitem "periods" begin
    p = Rembus.Now
    @test p == 0

    p = Rembus.LastReceived
    @test isinf(p)
end

@testitem "getargs" begin
    @test Rembus.getargs(nothing) == []
end

@testitem "shutdown_broker_error" begin
    # In case of failure just log a warning.
    # The API expect a broker argument, in this case it fails.
    Rembus.shutdown_broker(missing)
end

@testitem "showerror" begin
    router = Rembus.Router{Rembus.Twin}("test_router")
    router.settings.stacktrace = true
    twin = Rembus.Twin(Rembus.RbURL("component1"), router)
    e = ErrorException("test error")
    Rembus.dumperror(twin, e)
end

@testitem "string_to_enum" begin
    @test Rembus.string_to_enum("anonymous") === Rembus.anonymous
    @test Rembus.string_to_enum("authenticated") === Rembus.authenticated
    @test_throws ErrorException Rembus.string_to_enum("unknown")
end

@testitem "set_policy" begin
    router = Rembus.Router{Rembus.Twin}("test_router")

    @test isnothing(Rembus.set_policy(router, "first_up"))
    @test isnothing(Rembus.set_policy(router, "round_robin"))
    @test isnothing(Rembus.set_policy(router, "less_busy"))
    @test_throws ErrorException Rembus.set_policy(router, :invalid_policy)
end

@testitem "topic_auth_storage" begin
    router = Rembus.get_router(name="myrouter", http=6754)
    cfg = Dict("mytopic" => Dict("mycid" => true))
    router.topic_auth = cfg
    Rembus.save_topic_auth(router, router.con)

    router.topic_auth = Dict()
    Rembus.load_topic_auth(router, router.con)
    @test router.topic_auth == cfg

    shutdown()
end

@testitem "log_to_file" begin
    using Preferences
    logfile = joinpath(tempdir(), "test.log")
    ENV["BROKER_LOG"] = logfile

    # init log
    Rembus.logging("debug")

    # log something
    @info "this is a info log"
    @warn "this is a warn log"

    @test isfile(logfile)

    # reset log to default
    ENV["BROKER_LOG"] = "stdout"
    Rembus.logging("info")
end

@testitem "node" begin
    node = Rembus.Node("ws://myhost:8765/mycid")
    @test node.protocol === :ws
end

@testitem "get_meta" begin
    io = IOBuffer(encode([Rembus.TYPE_IDENTITY, 2, 3]))
    header = read(io, UInt8)
    @test isempty(Rembus.get_meta(io, header))

    io = IOBuffer(encode([Rembus.TYPE_IDENTITY, 2, 3, Dict("ws" => 8000)]))
    header = read(io, UInt8)
    for i in 1:3
        v = Rembus.decode_internal(io)
    end
    meta = Rembus.get_meta(io, header)
    @test haskey(meta, "ws")
end

@testitem "component_starts_with_slash" begin
    for str in ["/mycomponent", "//localhost/mycomponent", "mycomponent/"]
        url = Rembus.RbURL(str)
        @test url.id === "mycomponent"
    end
end

@testitem "wait for shutdown" begin
    rb = component(ws=9999)
    t = time()
    Timer(t -> shutdown(rb), 2)
    wait(rb)
    @test time() - t > 2.0
end

@testitem "no message dir" begin
    router = Rembus.get_router(name="test_router_2")
    mdir = Rembus.messages_dir(router)
    if isdir(mdir)
        rm(mdir, recursive=true)
    end

    @test !isdir(mdir)
    shutdown()
end
