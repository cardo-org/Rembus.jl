include("../utils.jl")

yes() = "yes"
no() = "no"

function start_servers()
    s1 = server(ws=7000)
    expose(s1, "myservice", yes)

    s2 = server(ws=7001)
    expose(s2, "myservice", no)
end

function many_responses()
    rb = connect(["ws://:7000", "ws://:6001", "ws://:7001"], :all)
    results = rpc(rb, "myservice", timeout=3, exceptionerror=false)
    @test isequal(results, ["yes", missing, "no"])
    close(rb)
end


@info "[test_rpc_all] start"
try
    start_servers()
    many_responses()
catch e
    @error "[test_rpc_all] error: $e"
    @test false
finally
    shutdown()
end

@info "[test_rpc_all] stop"
