include("../utils.jl")

function run()
    for cid in ["tcp://:8001/aaa", "ws://:8000/bbb", "zmq://:8002/ccc"]
        rb = connect(cid)
        @test isconnected(rb) === true

        close(rb)
        @test !isconnected(rb) === true
    end
end

execute(run, "test_connect")
