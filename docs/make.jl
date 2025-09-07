using Rembus
using Documenter
using DocumenterMermaid
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
    repo=Documenter.Remotes.GitHub("cardo-org", "Rembus.jl"),
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
        "Component API" => "api.md",
        "Macro-based API" => "macro_api.md",
        "HTTP Rest API" => "http_api.md",
        "JSON-RPC" => "json-rpc.md",
        "Client-Server" => "brokerless.md",
        "Fault-tolerance features" => "fault_tolerance.md",
        "Security" => "security.md",
        "Configuration" => "configuration.md",
    ],
)

deploydocs(; repo="github.com/cardo-org/Rembus.jl", devbranch="main")
