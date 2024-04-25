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
    fn = Rembus.key_file(cid)
    open(fn, create=true, write=true) do f
        write(f, "bbb")
    end
end

function run_embedded()
    try
        rb = embedded()
        serve(rb, wait=false, exit_when_done=false)

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
