include("../utils.jl")

target_name = "worker"

function install()

    myservice_code = """
function myservice(x, y)
    return x + y
end
"""

    mytopic_code = """
function mytopic(data)
   @info "[swdistribution] mytopic:\$data"
end
"""
    node = component("orchestrator")
    target_node = component(target_name)

    lst = direct(node, target_name, "julia_service_list", false)
    @test length(lst) == 0

    res = direct(
        node,
        target_name,
        "julia_service_install",
        Dict("name" => "myservice", "content" => myservice_code)
    )
    @test res == "ok"

    res = direct(
        node,
        target_name,
        "julia_service_install",
        Dict("name" => "myservice", "content" => myservice_code, "tag" => "v1.0.0")
    )
    @test res == "ok"

    res = direct(
        node,
        target_name,
        "julia_subscriber_install",
        Dict("name" => "mytopic", "content" => mytopic_code)
    )

    @test res == "ok"

    # test missing parameters
    @test_throws RpcMethodException direct(
        node,
        target_name,
        "julia_service_install",
        Dict("name" => "service_name")
    )
    @test_throws RpcMethodException direct(
        node,
        target_name,
        "julia_service_install",
        Dict("content" => myservice_code)
    )

    lst = direct(node, target_name, "julia_service_list", false)
    @test length(lst) == 1
    @test lst[1]["name"] == "myservice_v1.0.0.jl"

    lst = direct(node, target_name, "julia_service_list", true)
    @test length(lst) == 1
    @test lst[1]["content"] == myservice_code

    close(node)
    close(target_node)
end

function run()
    x = 1.0
    y = 2.0
    target_node = component("worker")
    cli = component("cli")

    # wait for the service to be installed and available
    sleep(2)
    result = direct(cli, target_name, "myservice", x, y)
    @test result == 3.0

    publish(cli, "mytopic", "Check this message in the log output")
    close(cli)
    close(target_node)
end

function uninstall()
    node = component("orchestrator")
    target_node = component("worker")

    res = direct(node, target_name, "julia_service_uninstall", "myservice")
    @test res == "ok"

    res = direct(node, target_name, "julia_subscriber_uninstall", "mytopic")
    @test res == "ok"

    path = joinpath(Rembus.rembus_dir(), "worker", "src", "services", "myservice.jl")
    @test !isfile(path)

    path = joinpath(Rembus.rembus_dir(), "worker", "src", "subscribers", "mytopic.jl")
    @test !isfile(path)

    close(node)
    close(target_node)
end

rm(
    joinpath(Rembus.rembus_dir(), "worker", "src", "services"),
    recursive=true,
    force=true
)

# Install
execute(install, "swdistribution")

# Invoke
execute(run, "swdistribution", reset=false)

# Uninistall
execute(uninstall, "swdistribution", reset=false)
