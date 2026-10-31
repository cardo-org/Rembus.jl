include("../utils.jl")

# a value that cannot be encoded: CBOR has no 128-bit integers
unencodable() = Int128(2)^100
sum_one(x) = x + 1
dual(x) = x * 2

function run()
    impl = connect("response_impl")
    client = connect("response_client")

    # an unencodable return value is answered with status 40
    expose(impl, "unencodable", unencodable)
    expose(impl, "sum_one", sum_one)
    @test_throws Rembus.RpcMethodException rpc(client, "unencodable")
    @test rpc(client, "sum_one", 1) == 2

    # a method that is also subscribed locally is still answered
    expose(impl, "dual", dual)
    subscribe(impl, "dual", dual)
    @test rpc(client, "dual", 2) == 4
end

execute(run, "response_always")
