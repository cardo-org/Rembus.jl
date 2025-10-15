include("../utils.jl")

function testcase(issecure)
    try
        if issecure
            node_url = "tls://$(gethostname()):8011/pub"
            rb = broker(secure=true, tcp=8011)
        else
            node_url = "tcp://:8011/pub"
            rb = broker(tcp=8011)
        end

        # If broker does not support a protocol then islistening return false.
        @test !Rembus.islistening(rb, protocol=[:ws], wait=1)
        @test Rembus.islistening(rb, protocol=[:tcp], wait=10)

        node = connect(Rembus.RbURL(node_url), name="component")
        ver = rpc(node, "version")
        @test ver == Rembus.VERSION

        @test !Rembus.isconnectionerror(node.socket, ErrorException("logic error"))
        @test !Rembus.close_is_ok(node.socket, Rembus.WrongTcpPacket())
        @test Rembus.close_is_ok(node.socket, EOFError())
    catch e
        @error "[tcp_connect] error: $e"
        @test false
    finally
        shutdown()
    end
end

@info "[tcp_connect] start"
testcase(false)

if Base.Sys.iswindows()
    @info "Windows platform detected: skipping test_https"
else
    test_keystore = joinpath(tempdir(), "keystore")
    script = joinpath(@__DIR__, "..", "..", "bin", "init_keystore")
    ENV["REMBUS_KEYSTORE"] = test_keystore
    ENV["HTTP_CA_BUNDLE"] = joinpath(test_keystore, REMBUS_CA)
    try
        Base.run(`$script -k $test_keystore -n $(gethostname())`)
        testcase(true)
    catch e
        @error "[tcp_connect] error: $e"
        @test false
    finally
        delete!(ENV, "REMBUS_KEYSTORE")
        delete!(ENV, "HTTP_CA_BUNDLE")
        rm(test_keystore, recursive=true, force=true)
    end
end

@info "[tcp_connect] end"
