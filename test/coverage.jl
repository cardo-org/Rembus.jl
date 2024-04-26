using Pkg
using Coverage

try
    Pkg.test("Rembus", coverage=true)
finally
    coverage = process_folder()
    LCOV.writefile("lcov.info", coverage)
end

for dir in [
    "src",
    "test",
    "test/api",
    "test/broker_plugin",
    "test/auth",
    "test/connect",
    "test/embedded",
    "test/integration",
    "test/park",
    "test/private",
    "test/unit"
]
    foreach(rm, filter(endswith(".cov"), readdir(dir, join=true)))
end
