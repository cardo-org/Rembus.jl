include("../utils.jl")

function run()
    for cid in ["tls://:8001/aaa", "wss://:8000/bbb"]
        rb = connect(cid)

        @test isconnected(rb) === true

        close(rb)
        @test !isconnected(rb) === true
    end
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test-tls_connect"
else
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore

    # reset the default value set by ws_connect()
    delete!(ENV, "HTTP_CA_BUNDLE")
    try
        Base.run(`$script -k $test_keystore`)
        args = Dict("secure" => true)
        execute(run, "test_tls_connect", args=args)
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        rm(test_keystore, recursive=true, force=true)
    end
end
