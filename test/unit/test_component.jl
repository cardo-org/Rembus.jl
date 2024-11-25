using Rembus
using Test

ENV["REMBUS_BASE_URL"] = "tcp://broker.org:8001"

c = Rembus.RbURL("myc")
@test c.port == 8001
@test c.host == "broker.org"
@test c.protocol == :tcp
@test Rembus.brokerurl(c) == "tcp://broker.org:8001"

c = Rembus.RbURL("zmq://broker.org:8002/myc")
@test c.id == "myc"
@test c.protocol == :zmq
@test c.host == "broker.org"
@test Rembus.brokerurl(c) == "tcp://broker.org:8002"

c = Rembus.RbURL("zmq://:8002/myc")
@test c.id == "myc"
@test c.protocol == :zmq
@test c.host == "broker.org"
@test Rembus.brokerurl(c) == "tcp://broker.org:8002"

delete!(ENV, "REMBUS_BASE_URL")

c = Rembus.RbURL("myc")
@test c.port == 8000
@test c.host == "127.0.0.1"
@test c.protocol == :ws
@test Rembus.brokerurl(c) == "ws://127.0.0.1:8000"

c = Rembus.RbURL("ws://127.0.0.1:8000")
@test !Rembus.hasname(c)
@test c.port == 8000
@test c.host == "127.0.0.1"
@test c.protocol == :ws
@test Rembus.brokerurl(c) == "ws://127.0.0.1:8000"

@test_throws ErrorException("wrong url xyz://host: unknown protocol xyz") Rembus.RbURL("xyz://host")
