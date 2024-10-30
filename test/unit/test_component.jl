using Rembus
using Test

ENV["REMBUS_BASE_URL"] = "tcp://caronte.org:8001"

c = Rembus.Component("myc")
@test c.port == 8001
@test c.host == "caronte.org"
@test c.protocol == :tcp
@test Rembus.brokerurl(c) == "tcp://caronte.org:8001"

c = Rembus.Component("zmq://caronte.org:8002/myc")
@test c.id == "myc"
@test c.protocol == :zmq
@test c.host == "caronte.org"
@test Rembus.brokerurl(c) == "tcp://caronte.org:8002"

c = Rembus.Component("zmq://:8002/myc")
@test c.id == "myc"
@test c.protocol == :zmq
@test c.host == "caronte.org"
@test Rembus.brokerurl(c) == "tcp://caronte.org:8002"

delete!(ENV, "REMBUS_BASE_URL")

c = Rembus.Component("myc")
@test c.port == 8000
@test c.host == "127.0.0.1"
@test c.protocol == :ws
@test Rembus.brokerurl(c) == "ws://127.0.0.1:8000"

c = Rembus.Component("ws://127.0.0.1:8000")
@test !Rembus.hasname(c)
@test c.port == 8000
@test c.host == "127.0.0.1"
@test c.protocol == :ws
@test Rembus.brokerurl(c) == "ws://127.0.0.1:8000"

@test_throws ErrorException("wrong url xyz://host: unknown protocol xyz") Rembus.Component("xyz://host")
