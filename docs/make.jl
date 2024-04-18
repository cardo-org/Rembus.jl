using Rembus
using Documenter
using Weave

#const EXAMPLES_DIR = "examples"
#
#weaved_files = ["stop_process.jl"]
#
#for fn in weaved_files
#    weave(joinpath(EXAMPLES_DIR, fn); doctype="github", out_path=joinpath("docs", "src"))
#end

DocMeta.setdocmeta!(Rembus, :DocTestSetup, :(using Rembus); recursive=true)

makedocs(;
    modules=[Rembus],
    authors="Attilio Donà",
    repo="https://github.com/cardo-org/Rembus.jl/blob/{commit}{path}#{line}",
    sitename="Rembus.jl",
    doctest=true,
    format=Documenter.HTML(;
        prettyurls=get(ENV, "CI", "false") == "true",
        canonical="https://cardo-org.github.io/Rembus.jl",
        edit_link="main",
        assets=String[],
    ),
    pages=[
        "Home" => "index.md",
        "Configuration" => "configuration.md",
        "Supervised API" => "supervised_api.md",
        "Unsupervised API" => "unsupervised_api.md",
        "Fault-tolerance features" => "fault_tolerance.md",
    ],
)

deploydocs(; repo="github.com/cardo-org/Rembus.jl", devbranch="main")
