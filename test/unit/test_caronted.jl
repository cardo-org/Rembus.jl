using Rembus
using Test

@async Rembus.brokerd()
sleep(0.1)
shutdown()
