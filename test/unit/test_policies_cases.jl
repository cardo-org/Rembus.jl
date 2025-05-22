using Rembus
using TestItems

@testitem "round_robin" begin
    tenant = "."

    mutable struct StubTwin <: Rembus.AbstractTwin
        id::String
        sts::Bool
    end

    Rembus.domain(::StubTwin) = tenant

    Rembus.isopen(t::StubTwin) = t.sts

    router = Rembus.Router{Rembus.Twin}("test_router")
    topic = "topic"
    t1 = StubTwin("t1", true)
    t2 = StubTwin("t2", false)
    t3 = StubTwin("t3", false)
    implementors = [t1, t2, t3]
    target = Rembus.round_robin(router, tenant, topic, implementors)
    @test target == t1
    target = Rembus.round_robin(router, tenant, topic, implementors)
    @test target == t1
end

@testitem "less_busy" begin
    tenant = "."

    struct StubSocket <: Rembus.AbstractPlainSocket
        out::String
    end

    Rembus.isopen(s::StubSocket) = true

    router = Rembus.Router{Rembus.Twin}("test_router")
    topic = "topic"
    t1 = Rembus.Twin(Rembus.RbURL("t1"), router)
    t1.socket = StubSocket("1")
    implementors = [t1]
    target = Rembus.less_busy(router, tenant, topic, implementors)
    @test target == t1
end
