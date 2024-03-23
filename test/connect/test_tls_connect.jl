include("../utils.jl")

function run()
    for cid in ["tls://:8001/aaa", "wss://:8000/bbb"]
        rb = connect(cid)

        @test isconnected(rb) === true

        close(rb)
        @test !isconnected(rb) === true
    end
end

# client env
ENV["HTTP_CA_BUNDLE"] = joinpath(Rembus.keystore_dir(), "rembus-ca.crt")

args = Dict("secure" => true)
execute(run, "test_tls_connect", args=args)

delete!(ENV, "HTTP_CA_BUNDLE")
