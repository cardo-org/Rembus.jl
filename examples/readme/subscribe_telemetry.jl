# Telemetry subscriber example.
#
# This component connects to a local Rembus broker and subscribes to telemetry
# messages matching a topic space. All received telemetry messages are printed
# to the console.
#
# Typical usage:
#     examples/readme/subscribe_telemetry.jl --name mymeter --topic "**/telemetry"
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
        help = "Component name"
        default = "mymeter"
        arg_type = String

        "--topic", "-t"
        help = "Topic space to subscribe to"
        default = "**/telemetry"
        arg_type = String

        "--debug", "-d"
        help = "Enable debug logging"
        action = :store_true
    end

    return parse_args(settings)
end

# ---------------------------------------------------------------------------
# Telemetry callback
# ---------------------------------------------------------------------------
function telemetry(topic, payload; ctx=nothing, node=nothing)
    println("📡 telemetry on $topic: $payload")
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

    println("✅ Subscribed to topic space: $topic")
    println("⏳ Waiting for telemetry (Ctrl+C to stop)")

    return node
end

node = main()
wait(node)
