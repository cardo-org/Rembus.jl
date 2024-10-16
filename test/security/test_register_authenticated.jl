include("../utils.jl")

pin = "11223344"
node = "authenticated_node"

function init(pin)
    broker_dir = Rembus.broker_dir(BROKER_NAME)
    df = DataFrame(pin=String[pin])
    if !isdir(broker_dir)
        mkdir(broker_dir)
    end
    Rembus.save_tenants(broker_dir, arraytable(df))
end

function run()
    register(node, pin)
end

try
    execute(run, "test_register_authenticated", mode="authenticated", setup=() -> init(pin))
    @test true
catch e
    @error "[test_register_authenticated]: $e"
    @test false
finally
end
