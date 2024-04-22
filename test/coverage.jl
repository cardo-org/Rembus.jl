using Pkg
using Coverage

Pkg.test("Rembus", coverage=true)
coverage = process_folder()
LCOV.writefile("lcov.info", coverage)

for dir in ["src", "test"]
    foreach(rm, filter(endswith(".cov"), readdir(dir, join=true)))
end
