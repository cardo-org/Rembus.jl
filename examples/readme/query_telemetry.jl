# Telemetry data at rest query example.
#
# This component connects to a local Rembus broker query the stored telemetry
# data in the DuckDB backend. It retrieves and prints all telemetry records for
# a specific sensor name.
#
# Typical usage:
#     examples/readme/query_telemetry.jl --name mymeter --topic "**/telemetry"
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

# ---------------------------------------------------------------------------
# Command-line parsing
# ---------------------------------------------------------------------------
function command_line()
    settings = ArgParseSettings()

    @add_arg_table! settings begin
        "--name", "-n"
        help = "Sensor name"
        default = "mymeter"
        arg_type = String

        "--debug", "-d"
        help = "Enable debug logging"
        action = :store_true
    end

    return parse_args(settings)
end

# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------
function main()
    args = command_line()

    if args["debug"]
        Rembus.debug!()
    end

    name = args["name"]
    topic = args["topic"]

    # Connect to a local broker (default: localhost:8000)
    node = component(name)

    subscribe(node, topic, telemetry)

    df = rpc(node, "query_telemetry")

    println("🟢 telemetry at rest for $name:")
    println(df)

    close(node)
end

main()
