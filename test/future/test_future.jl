include("../utils.jl")

function run()
    rb = connect()

    future = rpc_future(rb, "version")

    response = fetch_response(future)

    @test response == Rembus.VERSION

    future = rpc_future(rb, "unknow_service")
    #@test_throws RpcMethodNotFound fetch_response(future)
    fetch_response(future)

    close(rb)
end

execute(run, "test_future")
