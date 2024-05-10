using Rembus
using Test

@async Rembus.caronted()
sleep(0.1)
shutdown()
