# Sensor provisioning example.
#
# This component connects to a local Rembus broker and publishes to sensor
# topic.
#
# Typical usage:
#     examples/readme/publish_sensor.jl --name mygateway --topic "belluno/HVAC/agordo.sala1/sensor"
#
#!/bin/bash
#=
SDIR=$( dirname -- "${BASH_SOURCE[0]}" )
BINDIR=$( cd -- $SDIR &> /dev/null && pwd )
exec julia -t auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using Rembus
using ArgParse

function command_line()
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--name", "-n"
        help = "component name"
        default = "mygateway"
        arg_type = String
        "--topic", "-t"
        help = "the topic to publish the provisioning"
        default = "belluno/HVAC/agordo.sala1/sensor"
        arg_type = String
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
    end
    return parse_args(s)
end

args = command_line()

# Connect to a local component listening to port 8000
pub = component(args["name"])

# Send a sensor message: think of it as a sensor provisioning message.
# Here the topic contains all the sensor informations,
# therefore there isn't a payload.
publish(pub, args["topic"])

# Close the connection.
close(pub)
