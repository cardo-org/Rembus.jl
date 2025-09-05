# 🔵 stats.jl
using Statistics
using Rembus

# Return a stat summary of dataframes
# values from the value column.
function stats(df)
    return Dict(
        "min" => min(df.value...),
        "max" => max(df.value...),
        "mean" => mean(df.value),
        "std" => std(df.value)
    )
end

rb = component()

expose(rb, stats)

println("up and running")
wait(rb)
