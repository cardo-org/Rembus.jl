include("../utils.jl")

function generate_key(cid)
    mkpath(Rembus.keys_dir())
    private_fn = Rembus.pkfile(cid)

    if isfile(private_fn)
        rm(private_fn)
    end

    cmd = `ssh-keygen -f $private_fn -m PEM -b 2048 -N ''`
    Base.run(cmd)
    return private_fn
end

function setup(cid)
    private_fn = generate_key(cid)
    open(Rembus.key_file(cid), "w") do f
        write(f, read(`ssh-keygen -f $private_fn -e -m PEM`))
    end

    # generate another private key
    generate_key(cid)
    rm("$private_fn.pub")
end

function teardown(cid)
    rm(Rembus.pkfile(cid))
    rm(Rembus.key_file(cid))
end

function run(cid)
    try
        connect("wss://:9000/$cid")
    catch e
        @info "[test_wrong-secret] expected error: $e"
        @test isa(e, RembusError)
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
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, Rembus.REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore`)
        execute(
            () -> run(cid), "test_wrong_secret",
            setup=() -> setup(cid),
            args=Dict("secure" => true, "ws" => 9000)
        )

    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
        teardown(cid)
    end
end
