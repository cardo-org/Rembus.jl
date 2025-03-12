include("../utils.jl")
using DataFrames

myservice(df1, df2) = [df1, df2];

function run()
    Rembus.request_timeout!(30)
    server = component("dataframe_myserver")
    expose(server, myservice)

    df1 = DataFrame(:a => 1:10)
    df2 = DataFrame(:a => 1:1000)
    df3 = DataFrame(:a => 1:1_000_000)
    for url in ["tcp://127.0.0.1:8001/dataframe_c1"]
        rb = connect(url)
        # HEADER_LEN2
        response = rpc(rb, "myservice", df1, df2)
        @test response == [df1, df2]

        # HEADER_LEN4
        response = rpc(rb, "myservice", df2, df3)
        @test response == [df2, df3]
    end

    close(server)
end

execute(run, "dataframe")
