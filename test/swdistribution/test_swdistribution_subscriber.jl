include("../utils.jl")

function install()
    mytopic_code = """
function mytopic(data)
   @info "[swdistribution_subscriber] mytopic:\$data"
end
"""
    subscriber_path = joinpath(
        Rembus.broker_dir("swdistribution_subscriber"), "src", "subscribers", "mytopic_main.jl")

    node = component("orchestrator")

    lst = rpc(node, "julia_subscriber_list", false)
    @test length(lst) == 0

    res = rpc(
        node,
        "julia_subscriber_install",
        Dict("name" => "mytopic", "content" => mytopic_code)
    )
    @test res == "ok"
    @test read(subscriber_path, String) == mytopic_code


    lst = rpc(node, "julia_subscriber_list", false)
    @test length(lst) == 1
    @test lst[1]["name"] == "mytopic_main.jl"

    close(node)
end

function run()
    x = 1.0
    y = 2.0
    cli = component("cli")
    publish(cli, "mytopic", "Check this message in the log output")
    close(cli)
end

function uninstall()
    node = component("orchestrator")

    res = rpc(node, "julia_subscriber_uninstall", "mytopic")
    @test res == "ok"

    path = joinpath(Rembus.broker_dir("swdistribution_subscriber"), "src", "subscribers", "mytopic_v1.0.0.jl")
    @test !isfile(path)

    path = joinpath(Rembus.broker_dir("swdistribution_subscriber"), "src", "subscribers", "mytopic.jl")
    @test !isfile(path)

    close(node)
end

rm(
    joinpath(Rembus.broker_dir("swdistribution_subscriber"), "src", "subscribers"),
    recursive=true,
    force=true
)

# Install
execute(install, "swdistribution_subscriber")

# Invoke
execute(run, "swdistribution_subscriber", reset=false)

# Uninistall
execute(uninstall, "swdistribution_subscriber", reset=false)
