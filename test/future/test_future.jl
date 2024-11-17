include("../utils.jl")

function run()
    rb = connect()

    future = rpc(rb, "version", wait=false)

    response = fetch_response(future)

    @test response == Rembus.VERSION

    future = rpc(rb, "unknow_service", wait=false)
    @test_throws RembusError fetch_response(future)

    close(rb)
end

execute(run, "test_future")
