#!/usr/bin/env julia
"""
Rembus tutorial example: remote Julia function deployment and execution.

This script demonstrates how to:
1. Generate a synthetic telemetry dataset using DataFrames.jl
2. Define a Julia function (`stats`) operating on a DataFrame
3. Attempt to invoke the function remotely via Rembus
4. Dynamically deploy the function source code to a remote node
5. Invoke the deployed function and retrieve the result

The example mimics a telemetry/monitoring use case with sensor metrics.
"""

using ArgParse
using Random
using Dates
using DataFrames
using Statistics
using Rembus

stats_src = """
using DataFrames, Statistics

function stats(df::DataFrame)
    return combine(
        groupby(df, :kpi),
        :value => mean   => :mean,
        :value => minimum => :min,
        :value => median => :median,
        :value => maximum => :max,
        :value => length => :count,
    )
end
"""

"""
    create_df() -> DataFrame

Create a synthetic telemetry dataset.

The dataset simulates multiple sensors producing different KPIs
(temperature, pressure, humidity) over time.

# Returns
A DataFrame with the following schema:
- `name::String`  : sensor identifier
- `kpi::String`   : metric name
- `ts::String`  : ISO formatted timestamp of the measurement
- `value::Float64`: measured value
"""
function create_df(n_rows)
    sensors = ["sensor_$(i)" for i in 1:10]
    kpis = ["temperature", "pressure", "humidity"]
    start_ts = DateTime(2026, 1, 1)

    name = String[]
    kpi = String[]
    ts = String[]
    value = Float64[]

    for i in 0:(n_rows-1)
        s = rand(sensors)
        k = rand(kpis)

        v = if k == "temperature"
            rand(15.0:0.01:35.0)
        elseif k == "pressure"
            rand(980.0:0.01:1050.0)
        else # humidity
            rand(20.0:0.01:90.0)
        end

        push!(name, s)
        push!(kpi, k)
        push!(ts, string(start_ts + Minute(i)))
        push!(value, round(v, digits=2))
    end

    return DataFrame(
        name=name,
        kpi=kpi,
        ts=ts,
        value=value,
    )
end


"""
Main application entry point.

This function:
1. Connects to a Rembus node
2. Generates a dataset
3. Tries to invoke a remote function that is not yet installed
4. Deploys the function source code remotely
5. Calls the function again and prints the result
"""
function main(cli)
    s = ArgParseSettings()
    @add_arg_table s begin
        "--install"
        help = "Install the stats service on the remote node"
        action = :store_true

        "--rows"
        help = "Number of rows to generate in the synthetic dataset (default: 1000)"
        arg_type = Int
        default = 1000
    end

    parsed_args = parse_args(s)
    install_flag = get(parsed_args, "install", false)
    rows = parsed_args["rows"]


    df = create_df(rows)

    if install_flag
        # Deploy the Julia function as a remote service
        res = rpc(
            cli,
            "julia_service_install",  # software distribution topic
            "stats",                  # remote RPC service name
            stats_src,      # function source code
        )

        println("Service installation result: ", res)
    end

    # Invoke the newly installed remote function
    result = rpc(cli, "stats", df)
    println("Metrics summary:")
    println(result)

end

# Connect to a Rembus node
cli = component("myapp")
main(cli)
