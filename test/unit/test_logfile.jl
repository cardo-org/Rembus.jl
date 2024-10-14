using Rembus
using Test

logfile = "mycaronte.log"

Rembus.CONFIG.log_destination = logfile
logger = Rembus.logging()

@info "very important info"

open(logfile, "r") do f
    content = read(f, String)
    println("content=$content")
    @test occursin("very important info", content)
end

# on Windows avoid:
# IOError: unlink("mycaronte.log"): resource busy or locked (EBUSY)
close(logger.io)

rm(logfile)
