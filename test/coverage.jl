using Pkg
using Coverage

Pkg.test("Rembus", coverage=true)
coverage = process_folder()
LCOV.writefile("lcov.info", coverage)

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
