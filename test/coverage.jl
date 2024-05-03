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
    "test/broker_plugin",
    "test/auth",
    "test/connect",
    "test/embedded",
    "test/integration",
    "test/park",
    "test/private",
    "test/repl",
    "test/unit",
    "test/rbpool",
    "test/tcp",
    "test/zmq",
]
    foreach(rm, filter(endswith(".cov"), readdir(dir, join=true)))
end
