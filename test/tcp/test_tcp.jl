include("../utils.jl")

function run()
    component1 = connect(cid1)
    anonymous = connect()

    v = rpc(component1, "version")
    @test v == Rembus.VERSION
    close(component1)
    close(anonymous)
end

ENV["REMBUS_BASE_URL"] = "tcp://localhost:8001"

cid1 = "component2"
execute(run, "test_tcp")
delete!(ENV, "REMBUS_BASE_URL")
