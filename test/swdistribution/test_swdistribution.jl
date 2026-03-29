include("../utils.jl")

function install()
    myservice_code = """
function myservice(x, y)
    return x + y
end
"""
    service_path = joinpath(
        Rembus.broker_dir("swdistribution"), "src", "services", "myservice_main.jl")

    node = component("orchestrator")

    lst = rpc(node, "julia_service_list", false)
    @test length(lst) == 0

    res = rpc(
        node,
        "julia_service_install",
        Dict("name" => "myservice", "content" => myservice_code)
    )
    @test res == "ok"
    @test read(service_path, String) == myservice_code

    res = rpc(
        node,
        "julia_service_install",
        Dict("name" => "myservice", "content" => myservice_code, "tag" => "v1.0.0")
    )
    @test res == "ok"

    # test missing parameters
    @test_throws RpcMethodException rpc(
        node, "julia_service_install", Dict("name" => "service_name")
    )
    @test_throws RpcMethodException rpc(
        node, "julia_service_install", Dict("content" => myservice_code)
    )

    lst = rpc(node, "julia_service_list", false)
    @test length(lst) == 1
    @test lst[1]["name"] == "myservice_v1.0.0.jl"

    lst = rpc(node, "julia_service_list", true)
    @test length(lst) == 1
    @test lst[1]["content"] == myservice_code

    close(node)
end

function run()
    x = 1.0
    y = 2.0
    cli = component("cli")
    result = rpc(cli, "myservice", x, y)

    close(cli)
end

function uninstall()
    node = component("orchestrator")

    res = rpc(node, "julia_service_uninstall", "myservice")
    @test res == "ok"

    path = joinpath(Rembus.broker_dir("swdistribution"), "src", "services", "myservice.jl")
    @test !isfile(path)

    close(node)
end

rm(
    joinpath(Rembus.broker_dir("swdistribution"), "src", "services"),
    recursive=true,
    force=true
)

# Install
execute(install, "swdistribution")

# Invoke
execute(run, "swdistribution", reset=false)

# Uninistall
execute(uninstall, "swdistribution", reset=false)
