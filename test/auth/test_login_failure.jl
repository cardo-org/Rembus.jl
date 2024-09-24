include("../utils.jl")

using DataFrames

# set a mismatched shared secret
function init(cid)
    # component side
    pkfile = Rembus.pkfile(cid)
    open(pkfile, create=true, write=true) do f
        write(f, "aaa")
    end

    # broker side
    kdir = Rembus.keys_dir(BROKER_NAME)
    if !isdir(kdir)
        mkpath(kdir)
    end

    fn = Rembus.key_base(BROKER_NAME, cid)
    open(fn, create=true, write=true) do f
        write(f, "bbb")
    end
end

function run_embedded()
    try
        # set name to BROKER_NAME to get the secret
        rb = server()
        serve(rb, name=BROKER_NAME, wait=false)

        connect(cid)
        @test false
    catch e
        @error "run_embedded: $e"
        @test true
    finally
        shutdown()
    end
end

function run()
    try
        connect(url)
        @test false
    catch
        @test true
    end
end

cid = "regcomp"
url = "zmq://:8002/$cid"

setup() = init(cid)
execute(run, "test_login_failure", setup=setup)

@info "[test_login_embedded_failure] start"
run_embedded()
remove_keys(cid)
