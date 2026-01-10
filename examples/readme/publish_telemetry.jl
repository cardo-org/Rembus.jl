# Telemetry publishing example.
#
# This component connects to a local Rembus broker and publishes data to telemetry
# topic.
#
# Typical usage:
#     examples/readme/publish_telemetry.jl --name mygateway --topic "agordo.sala1/telemetry" --temperature 21.5 --pressure 995.0
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
        help = "the topic to publish telemetry"
        default = "agordo.sala1/telemetry"
        arg_type = String

        "--temperature", "-T"
        help = "the temperature reading"
        default = 18.5
        arg_type = Float64

        "--pressure", "-P"
        help = "the pressure reading"
        default = 980.0
        arg_type = Float64

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
publish(
    pub,
    args["topic"],
    Dict("temperature" => args["temperature"], "pressure" => args["pressure"])
)

# Close the connection.
close(pub)
