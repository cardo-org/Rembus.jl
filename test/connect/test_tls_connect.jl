include("../utils.jl")

function run()
    try
        for cid in ["tls://:8001/aaa", "wss://:8000/bbb"]
            rb = connect(cid)

            @test isconnected(rb) === true

            close(rb)
            @test !isconnected(rb) === true
        end
    catch e
        # throws an exception is connection fails.
        @test false
    end
end

function no_cacert()
    try
        @component "wss://:8000/bbb"
        @rpc version()
    catch e
        @test isa(e, ErrorException)
    end
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_tls_connect"
else
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        execute(run, "test_tls_connect", secure=true, tcp=8001, ws=8000)

        delete!(ENV, "HTTP_CA_BUNDLE")
        execute(no_cacert, "test_tls_connect_no_cacert", secure=true, tcp=8001, ws=8000)

        # test rembus_ca() method
        target_dir = joinpath(Rembus.rembus_dir(), "ca")
        mkpath(target_dir)
        mv(joinpath(test_keystore, REMBUS_CA), joinpath(target_dir, REMBUS_CA), force=true)
        execute(run, "test_tls_connect", secure=true, tcp=8001, ws=8000)

        # create a ca cert that does not signed the original certificate
        cacert = joinpath(target_dir, REMBUS_CA)
        Base.run(`openssl req -x509 \
            -sha256 -days 356 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=Rembus/C=IT/L=Trento" \
            -keyout /dev/null -out $cacert`)

        execute(no_cacert, "test_tls_connect_invalid_cacert", secure=true, tcp=8001, ws=8000)
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
        rm(joinpath(Rembus.rembus_dir(), "ca"), recursive=true, force=true)
    end
end
