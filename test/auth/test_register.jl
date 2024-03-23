include("../utils.jl")

using DataFrames

function init(uid, pin)
    df = DataFrame(pin=String[pin], uid=String[uid], name=["Test"], enabled=Bool[true])
    if !isdir(Rembus.CONFIG.db)
        mkdir(Rembus.CONFIG.db)
    end
    Rembus.save_owners(df)
end

function run()
    cmp = Rembus.Component(url)

    Rembus.register(url, uid, pin)

    # check configuration
    # token_app file contains the app component
    df = Rembus.load_token_app()
    @debug "token_app: $df" _group = :test
    @test df[df.app.==cmp.id, :app][1] === cmp.id
    @test df[df.app.==cmp.id, :uid][1] === uid

    # private key was created
    @test isfile(Rembus.pkfile(cmp.id))

    # public key was provisioned
    fname = Rembus.pubkey_file(cmp.id)
    @test basename(fname) === cmp.id

    client = tryconnect(url)

    try
        Rembus.unregister(client, cmp.id)

        df = Rembus.load_token_app()

        # the app component was removed from token_app file
        @test isempty(df[df.app.==cmp.id, :])

        # the public key was removed
        @test_throws ErrorException Rembus.pubkey_file(cmp.id)

        # the private key was removed
        @test isfile(Rembus.pkfile(cmp.id)) === false
    catch
        @test 0 == 1
        rethrow()
    finally
        close(client)
    end
end

uid = "rembus_user"
url = "zmq://:8002/regcomp"
pin = "11223344"

setup() = init(uid, pin)
execute(run, "test_register", setup=setup)
