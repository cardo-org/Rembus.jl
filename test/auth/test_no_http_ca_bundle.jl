include("../utils.jl")

function generate_key(cid)
    private_fn = Rembus.pkfile(cid)
    if isfile(private_fn)
        rm(private_fn)
    end

    cmd = `ssh-keygen -t rsa -f $private_fn -m PEM -b 2048 -N ''`
    Base.run(cmd)
    return private_fn
end

function setup(cid)
    private_fn = generate_key(cid)
    basename = Rembus.key_base(BROKER_NAME, cid)
    mv("$private_fn.pub", "$basename.rsa.pem", force=true)
end

function teardown(cid)
    rm(Rembus.pkfile(cid))
    rm(Rembus.key_file(BROKER_NAME, cid))
end

function run(cid)
    try
        connect("wss://:9000/$cid")
    catch e
        @info "[test_no_http_ca_bundle] expected error: $e"
        @test isa(e, Rembus.CABundleNotFound)
    end
end

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test-tls_connect"
else
    cid = "mycomponent"
    # create keystore
    test_keystore = "/tmp/keystore"
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    try
        Base.run(`$script -k $test_keystore`)
        execute(
            () -> run(cid), "test_no_http_ca_bundle",
            setup=() -> setup(cid),
            args=Dict("secure" => true, "ws" => 9000)
        )

    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        rm(test_keystore, recursive=true, force=true)
        teardown(cid)
    end
end
