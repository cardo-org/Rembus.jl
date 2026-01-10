# Rembus broker example with DuckDB backend.
# This example sets up a Rembus broker that uses DuckDB as the storage
# backend. The broker is configured with a schema that defines two tables:
# "sensor" and "telemetry". The broker listens for incoming messages and
# stores them in the DuckDB database according to the defined schema.
# Typical usage:
#     examples/readme/broker.jl
#
#!/bin/bash
#=
SDIR=$( dirname -- "${BASH_SOURCE[0]}" )
BINDIR=$( cd -- $SDIR &> /dev/null && pwd )
exec julia -t auto --color=no -e "include(popfirst!(ARGS))" \
 --project=$BINDIR/.. --startup-file=no "${BASH_SOURCE[0]}" "$@"
=#
using ArgParse
using DuckDB
using Rembus

# Enable debug logging (optional)
#Rembus.debug!()

schema_json = """
{
    "tables": [
        {
            "table": "sensor",
            "topic": ":site/:type/:dn/sensor",
            "columns": [
                {"col": "site", "type": "TEXT", "nullable": false},
                {"col": "type", "type": "TEXT", "nullable": false},
                {"col": "dn", "type": "TEXT"}
            ],
            "keys": ["dn"]
        },
        {
            "table": "telemetry",
            "topic": ":dn/telemetry",
            "columns": [
                {"col": "dn", "type": "TEXT"},
                {"col": "temperature", "type": "DOUBLE"},
                {"col": "pressure", "type": "DOUBLE"}
            ],
            "extras": {"recv_ts": "ts", "slot": "time_bucket"}
        }
    ]
}"""

# ---------------------------------------------------------------------------
# Command-line parsing
# ---------------------------------------------------------------------------
function command_line()
    settings = ArgParseSettings()

    @add_arg_table! settings begin
        "--debug", "-d"
        help = "Enable debug logging"
        action = :store_true
    end

    return parse_args(settings)
end

args = command_line()

if args["debug"]
    Rembus.debug!()
end

bro = broker(DuckDB.DB(), schema=schema_json)

println("🚀 DuckDB broker up and running")
wait(bro)
