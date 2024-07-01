include("../utils.jl")

using DataFrames

# tests: 3

function init(server_name, uid, pin)
    broker_dir = Rembus.broker_dir(server_name)
    df = DataFrame(pin=String[pin], uid=String[uid], name=["Test"], enabled=Bool[true])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_owners(broker_dir, df)
end


function run()
    cid = "mycid"
    uid = "userid"
    pin = "11223344"
    server_name = "server_test"

    init(server_name, uid, pin)

    srv = server()
    serve(srv, wait=false, args=Dict("name" => server_name))

    Rembus.register(cid, uid, pin)

    keyfn = joinpath(Rembus.keys_dir(server_name), cid)
    @test isfile(keyfn)

    rb = connect(cid)

    @test rpc(rb, "version") == Rembus.VERSION

    Rembus.unregister(rb, cid)

    ENV["REMBUS_TIMEOUT"] = 0
    @test_throws RembusTimeout Rembus.unregister(rb, cid)
    delete!(ENV, "REMBUS_TIMEOUT")

    @test !isfile(keyfn)

    close(rb)
    shutdown()
end

function login_failed()
    cid = "mycid"
    server_name = "server_test"
    kdir = Rembus.keys_dir(server_name)
    mkpath(kdir)
    server_side_secret = joinpath(kdir, cid)
    try
        write(server_side_secret, "aaa")
        write(Rembus.pkfile(cid), "bbb")

        srv = server()
        serve(srv, wait=false, args=Dict("name" => server_name))
        @test_throws RembusError connect(cid)
    finally
        rm(server_side_secret)
        rm(Rembus.pkfile(cid))
        shutdown()
    end

end

run()

login_failed()
