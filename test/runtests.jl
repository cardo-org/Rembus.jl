using Distributed
addprocs(2)

@everywhere using Rembus
@everywhere using Test
using SafeTestsets
using Visor

const GROUP = get(ENV, "GROUP", "all")

Rembus.CONFIG = Rembus.Settings()
Rembus.CONFIG.db = "/tmp/caronte_test"

mkpath(joinpath(Rembus.CONFIG.db, "apps"))

@testset "Rembus" begin
    @testset "unit" begin
        if GROUP == "all" || GROUP == "unit"
            @time @safetestset "twin" begin
                include("unit/test_twin.jl")
            end
            @time @safetestset "cbor" begin
                include("unit/test_cbor.jl")
            end
            @time @safetestset "component" begin
                include("unit/test_component.jl")
            end
            @time @safetestset "signature" begin
                include("unit/test_signature.jl")
            end
            @time @safetestset "balancer_round_robin" begin
                include("unit/test_round_robin.jl")
            end
            @time @safetestset "balancer_less_busy" begin
                include("unit/test_less_busy.jl")
            end
        end
        if GROUP == "all" || GROUP == "private"
            @time @safetestset "private_topic" begin
                include("private/test_private_topic.jl")
            end
        end
        if GROUP == "all" || GROUP == "embedded"
            @time @safetestset "embedded" begin
                include("embedded/test_embedded.jl")
            end
        end
        if GROUP == "ack"
            @time @safetestset "ws_ack" begin
                include("ack/test_ws_ack.jl")
            end
            @time @safetestset "zmq_ack" begin
                include("ack/test_zmq_ack.jl")
            end
        end
        if GROUP == "all" || GROUP == "connect"
            @time @safetestset "tls_connect" begin
                include("connect/test_tls_connect.jl")
            end
            @time @safetestset "connect" begin
                include("connect/test_connect.jl")
            end
        end
        if GROUP == "all" || GROUP == "integration"
            @time @safetestset "retroactive" begin
                include("integration/test_retroactive.jl")
            end
            @time @safetestset "process_fault" begin
                include("integration/test_process_fault.jl")
            end
            @time @safetestset "zmq_protocol_errors" begin
                include("integration/test_zmq_protocol_errors.jl")
            end
            @time @safetestset "round_robin" begin
                include("integration/test_round_robin.jl")
            end
            @time @safetestset "wrong_balancer" begin
                include("integration/test_wrong_balancer.jl")
            end
        end
        if GROUP == "all" || GROUP == "api"
            @time @safetestset "publish_api" begin
                include("api/test_publish.jl")
            end
            @time @safetestset "publish_macros" begin
                include("api/test_publish_macros.jl")
            end
            @time @safetestset "request_api" begin
                include("api/test_request.jl")
            end
            @time @safetestset "types" begin
                include("api/test_types.jl")
            end
            @time @safetestset "zmq" begin
                include("api/test_zmq.jl")
            end
            @time @safetestset "mixed" begin
                include("api/test_mixed.jl")
            end
        end
        if GROUP == "all" || GROUP == "auth"
            @time @safetestset "register" begin
                include("auth/test_register.jl")
            end
        end
        if GROUP == "all" || GROUP == "park"
            @time @safetestset "park" begin
                include("park/test_park.jl")
            end
            @time @safetestset "park" begin
                include("park/test_park_macro.jl")
            end
        end
    end
end
