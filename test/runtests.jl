using Rembus
using Test
using SafeTestsets
using Visor

const GROUP = get(ENV, "GROUP", "all")

if Sys.iswindows()
    rootdir = joinpath(get(ENV, "LOCALAPPDATA", "."), "caronte_test")
else
    rootdir = joinpath(get(ENV, "HOME", "."), "caronte_test")
end

Rembus.CONFIG = Rembus.Settings(rootdir)

rm(Rembus.CONFIG.db, force=true, recursive=true)

@testset "Rembus" begin
    if GROUP == "all" || GROUP == "unit"
        @time @safetestset "caronte" begin
            include("unit/test_caronte.jl")
        end
        @time @safetestset "caronted" begin
            include("unit/test_caronted.jl")
        end
        @time @safetestset "embedded" begin
            include("unit/test_embedded.jl")
        end
        @time @safetestset "messages" begin
            include("unit/test_messages.jl")
        end
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
    if GROUP == "all" || GROUP == "ack"
        @time @safetestset "simple_ack" begin
            include("ack/test_simple_ack.jl")
        end
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
        @time @safetestset "reconnect" begin
            include("connect/test_reconnect.jl")
        end
    end
    if GROUP == "all" || GROUP == "rbpool"
        @time @safetestset "rbpool" begin
            include("rbpool/test_rbpool.jl")
        end
    end
    if GROUP == "all" || GROUP == "config"
        @time @safetestset "config" begin
            include("config/test_broker_config.jl")
        end
    end
    if GROUP == "all" || GROUP == "integration"
        @time @safetestset "wrong_packet" begin
            include("integration/test_wrong_packet.jl")
        end
        @time @safetestset "rawlog" begin
            include("integration/test_rawlog.jl")
        end
        @time @safetestset "rembus_task" begin
            include("integration/test_rembus_task.jl")
        end
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
        @time @safetestset "less_busy" begin
            include("integration/test_less_busy.jl")
        end
        @time @safetestset "wrong_balancer" begin
            include("integration/test_wrong_balancer.jl")
        end
        @time @safetestset "admin_commands" begin
            include("integration/test_admin_commands.jl")
        end
        @time @safetestset "indefinite_len" begin
            include("integration/test_indefinite_len.jl")
        end
        @time @safetestset "unexpected_response" begin
            include("integration/test_unexpected_response.jl")
        end
        @time @safetestset "wrong_admin_command" begin
            include("integration/test_wrong_admin_command.jl")
        end
        @time @safetestset "forever" begin
            include("integration/test_forever.jl")
        end
    end
    if GROUP == "all" || GROUP == "api"
        @time @safetestset "component" begin
            include("api/test_component.jl")
        end
        @time @safetestset "simple_publish" begin
            include("api/test_simple_publish.jl")
        end
        @time @safetestset "conditional_publish" begin
            include("api/test_conditional_publish.jl")
        end
        @time @safetestset "publish_api" begin
            include("api/test_publish.jl")
        end
        @time @safetestset "publish_macros" begin
            include("api/test_publish_macros.jl")
        end
        @time @safetestset "publish_ack" begin
            include("api/test_publish_ack.jl")
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
        @time @safetestset "subscribe_glob" begin
            include("api/test_subscribe_glob.jl")
        end
    end
    if GROUP == "all" || GROUP == "auth"
        @time @safetestset "register" begin
            include("auth/test_register.jl")
        end
        @time @safetestset "login_failure" begin
            include("auth/test_login_failure.jl")
        end
        @time @safetestset "wrong_secret" begin
            include("auth/test_wrong_secret.jl")
        end
    end
    if GROUP == "all" || GROUP == "park"
        @time @safetestset "page_file" begin
            include("park/test_page_file.jl")
        end
        @time @safetestset "park" begin
            include("park/test_park.jl")
        end
        @time @safetestset "park_macro" begin
            include("park/test_park_macro.jl")
        end
    end
    if GROUP == "all" || GROUP == "zmq"
        @time @safetestset "zmq" begin
            include("zmq/test_zmq.jl")
        end
        @time @safetestset "zmq_nodealer" begin
            include("zmq/test_zmq_nodealer.jl")
        end
    end
    if GROUP == "all" || GROUP == "tcp"
        @time @safetestset "zmq" begin
            include("tcp/test_tcp.jl")
        end
    end
    if GROUP == "all" || GROUP == "broker"
        @time @safetestset "broker_plugin" begin
            include("broker_plugin/test_plugin.jl")
        end
    end
    if GROUP == "all" || GROUP == "errors"
        @time @safetestset "router_zmq_message" begin
            include("errors/test_router_zmq_message.jl")
        end
        @time @safetestset "dealer_zmq_message" begin
            include("errors/test_dealer_zmq_message.jl")
        end
        @time @safetestset "invalid_states" begin
            include("errors/test_invalid_states.jl")
        end
        @time @safetestset "errors" begin
            include("errors/test_rembus_errors.jl")
        end
        @time @safetestset "serve_ws_error" begin
            include("errors/test_serve_ws_error.jl")
        end
        @time @safetestset "serve_zmq_error" begin
            include("errors/test_serve_zmq_error.jl")
        end
        @time @safetestset "connection_error" begin
            include("errors/test_connection_error.jl")
        end
        @time @safetestset "transport_send_error" begin
            include("errors/test_transport_send_error.jl")
        end
    end
    if GROUP == "all" || GROUP == "repl"
        @time @safetestset "repl" begin
            include("repl/test_repl.jl")
        end
    end
end
