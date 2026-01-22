include("../utils.jl")

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
    service_path = joinpath(
        Rembus.broker_dir("swdistribution"), "src", "services", "myservice.jl")
    subscriber_path = joinpath(
        Rembus.broker_dir("swdistribution"), "src", "subscribers", "mytopic.jl")

    node = component("orchestrator")

    res = rpc(node, "julia_service_install", "myservice", myservice_code)
    @test res == "ok"
    @test read(service_path, String) == myservice_code

    res = rpc(node, "julia_subscriber_install", "mytopic", mytopic_code)
    @test res == "ok"
    @test read(subscriber_path, String) == mytopic_code

    close(node)
end

function run()
    x = 1.0
    y = 2.0
    cli = component("cli")
    result = rpc(cli, "myservice", x, y)

    publish(cli, "mytopic", "Check this message in the log output")
    close(cli)
end

function uninstall()
    node = component("orchestrator")

    res = rpc(node, "julia_service_uninstall", "myservice")
    @test res == "ok"

    res = rpc(node, "julia_subscriber_uninstall", "mytopic")
    @test res == "ok"

    path = joinpath(Rembus.broker_dir("swdistribution"), "src", "services", "myservice.jl")
    @test !isfile(path)

    path = joinpath(Rembus.broker_dir("swdistribution"), "src", "subscribers", "mytopic.jl")
    @test !isfile(path)

    close(node)
end

# Install
execute(install, "swdistribution")

# Invoke
execute(run, "swdistribution", reset=false)

# Uninistall
execute(uninstall, "swdistribution", reset=false)
