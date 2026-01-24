using Pkg
using Coverage

try
    Pkg.test("Rembus", coverage=true, julia_args=["--depwarn=no"])
catch e
    @error "coverage: $e"
finally
    coverage = process_folder()
    cov_ext = process_folder("ext")
    LCOV.writefile("lcov.info", [coverage; cov_ext])
end

for dir in [
    "src",
    "ext",
    "test",
    "test/ack",
    "test/api",
    "test/failovers",
    "test/duckdb",
    "test/mqtt",
    "test/keyspace",
    "test/offline",
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
