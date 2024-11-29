using Rembus
using Test

struct Ctx
    broker::Rembus.Twin
end

module Broker

using Rembus

# just to trigger an error when loading the module
export fake_function

end # module Broker

function run()
    broker(wait=false, plugin=Broker, ws=9000)
    Rembus.islistening(wait=2)
    process = from("broker")
    @test process.status === Visor.done
end

run()
