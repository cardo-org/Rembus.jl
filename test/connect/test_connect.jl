include("../utils.jl")

function run()
    # connect and reconnect
    for i in 1:2
        @debug "[test_connect]: connecting round $i" _group = :test
        for cid in ["tcp://:8001/aaa", "ws://:8000/bbb", "zmq://:8002/ccc"]
            rb = connect(cid)
            @test isconnected(rb) === true

            close(rb)
            @test !isconnected(rb) === true
        end
    end
end

execute(run, "test_connect")
