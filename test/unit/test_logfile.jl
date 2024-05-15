using Rembus
using Test

logfile = "mycaronte.log"

Rembus.CONFIG.log = logfile
Rembus.logging(debug=[])

@info "very important info"

open(logfile, "r") do f
    content = read(f, String)
    println("content=$content")
    @test occursin("very important info", content)
end

rm(logfile)
