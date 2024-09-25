include("../utils.jl")

function task(pd, router)
    router.process = pd
    for msg in pd.inbox
        @isshutdown(msg)
    end
end

function verify(cid, wrong_challenge=nothing)
    rb = Rembus.RBConnection(cid)

    challenge = [0x1, 0x2, 0x3, 0x4]
    Response = Rembus.ResMsg(1, Rembus.STS_SUCCESS, challenge)

    att = Rembus.attestate(rb, Response)

    if wrong_challenge === nothing
        server_challenge = challenge
    else
        server_challenge = wrong_challenge
    end

    router = Rembus.Router()
    proc = process("router", task, args=(router,))

    twin = Rembus.Twin(router, "twin")
    twin.session["challenge"] = server_challenge

    supervise([
            supervisor(BROKER_NAME, [
                proc,
                process(Rembus.twin_task, args=(twin,))
            ])
        ], wait=false)
    yield()

    Rembus.verify_signature(twin, att)
end

function run()
    cid = "plain_test"
    secret = "pippo"

    client_fn = Rembus.pkfile(cid)
    server_fn = Rembus.key_base(BROKER_NAME, cid)
    @info "secret file: $client_fn"

    # create client and server files
    for fn in [client_fn, server_fn]
        open(fn, "w") do f
            write(f, secret)
        end
    end

    isvalid = verify(cid)
    @test isvalid

    try
        verify(cid, [0x01])
        @test false
    catch e
        @test e.msg == "authentication failed"
    end

    cid = "private_test"

    # create private secret
    client_fn = Rembus.pkfile(cid)
    pubkey = Rembus.rsa_private_key(cid)
    mv("$(client_fn).tmp", client_fn, force=true)

    # create public secret
    server_fn = Rembus.key_base(BROKER_NAME, cid)
    open("$server_fn.rsa.pem", "w") do f
        write(f, pubkey)
    end

    isvalid = verify(cid)
    @test isvalid
end

testname = "test_signature"

try
    @info "[$testname] start"
    mkpath(Rembus.keys_dir(BROKER_NAME))
    run()
catch e
    @error "[$testname] error: $e"
    showerror(stdout, e, stacktrace())
finally
    rm(Rembus.broker_dir(BROKER_NAME), recursive=true)
end
