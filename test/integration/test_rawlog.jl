include("../utils.jl")

function run()
    Rembus.CONFIG.rawdump = true
    Rembus.CONFIG.metering = true

    rb = tryconnect("test_rawlog")
    v = rpc(rb, "version")
    @info "[test_rawdump] version=$v"
    @test isa(v, String)
    close(rb)
end

execute(run, "test_rawlog.jl")
