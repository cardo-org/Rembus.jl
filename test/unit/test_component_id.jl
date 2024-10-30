using Rembus
using Test

cfg = get(Base.get_preferences(), "Rembus", Dict())
cid = Rembus.component_id(cfg)
@test !Rembus.hasname(cid)

ENV["REMBUS_CID"] = "tcp://myhost:8999/mycomponent"
cfg = get(Base.get_preferences(), "Rembus", Dict())
cid = Rembus.component_id(cfg)
@test cid.id == "mycomponent"
delete!(ENV, "REMBUS_CID")
