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
    @test wait(twin) === nothing
    @test !Rembus.isconnectionerror(twin.socket, ErrorException("something nasty"))
end

@testitem "rembus_debug" begin
    ENV["REMBUS_DEBUG"] = "1"
    router = Rembus.Router{Rembus.Twin}("test_router")
    @test router.settings.log_level === Rembus.TRACE_DEBUG
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
    Rembus.rembus_dir!(joinpath(tempdir(), "rembus"))
    dir = joinpath(Rembus.rembus_dir(), bro, comp)
    @info "creating dir $dir"
    mkpath(dir)

    router = Rembus.Router{Rembus.Twin}(bro)
    fn = Rembus.acks_file(router, comp)
    if isfile(fn)
        rm(fn)
    end

    url = Rembus.RbURL(comp)
    twin = Rembus.Twin(url, router)
    msg = Rembus.PubSubMsg(twin, "topic", "data", Rembus.QOS2)
    twin.ackdf = Rembus.load_pubsub_received(router, url)
    Rembus.add_pubsub_id(twin, msg)
    Rembus.detach(twin)

    # reload the acks file
    twin2 = Rembus.Twin(url, router)
    twin2.ackdf = Rembus.load_pubsub_received(router, url)
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
    rb = broker()
    @test Rembus.islistening(rb, wait=10)
    shutdown()
end
@testitem "default_server" begin
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
    Rembus.stacktrace!()
    e = ErrorException("test error")
    Rembus.dumperror(e)
    Rembus.stacktrace!(false)
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
    router = Rembus.get_router(name="myrouter", http=8000)
    cfg = Dict("mytopic" => Dict("mycid" => true))
    router.topic_auth = cfg
    Rembus.save_topic_auth_table(router)

    router.topic_auth = Dict()
    Rembus.load_topic_auth_table(router)
    @test router.topic_auth == cfg

    shutdown()
end

@testitem "log_to_file" begin
    using Preferences
    logfile = joinpath(tempdir(), "test.log")
    set_preferences!(Rembus, "log_destination" => logfile, force=true)

    # init log
    Rembus.logging("debug")

    # log something
    @info "this is a info log"
    @warn "this is a warn log"

    @test isfile(logfile)

    # reset log to default
    set_preferences!(Rembus, "log_destination" => "stdout", force=true)
    Rembus.logging("info")
end

@testitem "node" begin
    node = Rembus.Node("ws://myhost:8765/mycid")
    @test node.protocol === :ws
end

@testitem "request_timeout" begin
    ENV["REMBUS_TIMEOUT"] = "10"
    @test Rembus.request_timeout() == 10
end
