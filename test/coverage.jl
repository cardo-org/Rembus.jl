using Pkg
using Coverage

try
    Pkg.test("Rembus", coverage=true)
catch e
    @error "coverage: $e"
finally
    coverage = process_folder()
    LCOV.writefile("lcov.info", coverage)
end

for dir in [
    "src",
    "test",
    "test/ack",
    "test/api",
    "test/broker",
    "test/twin",
    "test/errors",
    "test/security",
    "test/private",
    "test/repl",
    "test/unit",
    "test/tcp",
    "test/zmq",
    "test/ws",
    "test/http",
    "test/json-rpc",
    "test/prometheus",
]
    foreach(rm, filter(endswith(".cov"), readdir(dir, join=true)))
end
