include("../utils.jl")

broker_name = "connect_errors_secure"

function generate_key(cid)
    mkpath(Rembus.keys_dir(broker_name))
    private_fn = Rembus.pkfile(cid, create_dir=true)

    if isfile(private_fn)
        rm(private_fn)
    end

    cmd = `ssh-keygen -t rsa -f $private_fn -m PEM -b 2048 -N ''`
    Base.run(cmd)
    return private_fn
end

function generate_wrong_keys(cid)
    private_fn = generate_key(cid)
    basename = Rembus.key_base(broker_name, cid)
    open("$basename.rsa.pem", "w") do f
        write(f, read(`ssh-keygen -f $private_fn -e -m PEM`))
    end

    # generate another private key
    generate_key(cid)
    rm("$private_fn.pub")
end

function connect_fail()
    node_url = "ws://:5000/connect_errors_pub"
    try
        connect(node_url)
    catch e
        @test isa(e, HTTP.Exceptions.ConnectError)
    end

    try
        connect("zap://5001/connect_errors_pub")
    catch e
        @test e.msg === "wrong url zap://5001/connect_errors_pub: unknown protocol zap"
    end
end

function connect_secure()
    try
        for cid in ["tls://:8001/connect_errors_aaa", "wss://:8000/connect_errors_bbb"]
            rb = connect(cid)

            @test isopen(rb) === true

            shutdown(rb)
            @test !isopen(rb) === true
        end
    catch e
        showerror(stdout, e, catch_backtrace())
        # throws an exception is connection fails.
        @test false
    end
end

function no_cacert()
    try
        connect("tls://:8001")
        @test false
    catch e
        @test isa(e, Rembus.CABundleNotFound)
    end

    try
        connect("wss://:8000")
        @test false
    catch e
        @test isa(e, Rembus.CABundleNotFound)
    end

end

function invalid_cacert()
    try
        connect("tls://:8001")
        @test false
    catch e
        @test isa(e, Base.IOError)
        @test e.code == -9984
    end

    try
        connect("wss://:8000")
        @test false
    catch e
        @test isa(e, HTTP.Exceptions.ConnectError)
        @test isa(e.error, CapturedException)
    end
end

function wrong_keys(cid)
    try
        connect("tls://:8001/$cid")
        @test false
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code == Rembus.STS_GENERIC_ERROR
    end

    try
        connect("wss://:8000/$cid")
        @test false
    catch e
        @test isa(e, Rembus.RembusError)
        @test e.code == Rembus.STS_GENERIC_ERROR
    end
end

function missing_keys(cid)
    try
        connect("tls://:8001/$cid")
        @test false
    catch e
        @test isa(e, ErrorException)
    end

    try
        connect("wss://:8000/$cid")
        @test false
    catch e
        @test isa(e, ErrorException)
    end
end

@info "[start] test_connect_fail"
connect_fail()
@info "[stop] test_connect_fail"

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_tls_connect"
else
    # create keystore
    test_keystore = joinpath(tempdir(), "keystore")
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        execute(connect_secure, broker_name, secure=true, tcp=8001, ws=8000)

        delete!(ENV, "HTTP_CA_BUNDLE")
        execute(no_cacert, "connect_errors_no_cacert", secure=true, tcp=8001, ws=8000)

        # test rembus_ca() method
        target_dir = joinpath(Rembus.rembus_dir(), "ca")
        mkpath(target_dir)
        mv(joinpath(test_keystore, REMBUS_CA), joinpath(target_dir, REMBUS_CA), force=true)
        execute(connect_secure, "connect_errors_default_ca", secure=true, tcp=8001, ws=8000)

        cid = "connect_errors_wrong_keys"
        generate_wrong_keys(cid)
        execute(() -> wrong_keys(cid), broker_name, secure=true, tcp=8001, ws=8000)

        private_fn = Rembus.pkfile(cid)
        rm(private_fn)
        execute(() -> missing_keys(cid), broker_name, secure=true, tcp=8001, ws=8000)

        # create a ca cert that does not signed the original certificate
        cacert = joinpath(target_dir, REMBUS_CA)
        Base.run(`openssl req -x509 \
            -sha256 -days 356 \
            -nodes \
            -newkey rsa:2048 \
            -subj "/CN=Rembus/C=IT/L=Trento" \
            -keyout /dev/null -out $cacert`)

        execute(invalid_cacert, "connect_errors_invalid_cacert", secure=true, tcp=8001, ws=8000)
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
        rm(joinpath(Rembus.rembus_dir(), "ca"), recursive=true, force=true)
    end
end
