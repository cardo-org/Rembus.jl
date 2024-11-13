using Rembus
using Test
using SafeTestsets
using Visor

const GROUP = get(ENV, "GROUP", "all")

@testset "Rembus" begin
    if GROUP == "all" || GROUP == "unit"
        @time @safetestset "broker" begin
            include("unit/test_caronte.jl")
        end
        @time @safetestset "brokerd" begin
            include("unit/test_caronted.jl")
        end
        @time @safetestset "server" begin
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
        @time @safetestset "logfile" begin
            include("unit/test_logfile.jl")
        end
        @time @safetestset "component_id" begin
            include("unit/test_component_id.jl")
        end
        @time @safetestset "twin_topics" begin
            include("unit/test_twin_topics.jl")
        end
    end
    if GROUP == "all" || GROUP == "http"
        @time @safetestset "http" begin
            include("http/test_http.jl")
        end
        @time @safetestset "http_server" begin
            include("http/test_http_server.jl")
        end
        @time @safetestset "https" begin
            include("http/test_https.jl")
        end
        @time @safetestset "http_admin" begin
            include("http/test_http_admin.jl")
        end
    end
    if GROUP == "all" || GROUP == "private"
        @time @safetestset "private_topic" begin
            include("private/test_private_topic.jl")
        end
    end
    if GROUP == "all" || GROUP == "server"
        @time @safetestset "broker_add_server" begin
            include("server/test_broker_add_server.jl")
        end
        @time @safetestset "broker_server" begin
            include("server/test_broker_server.jl")
        end
        @time @safetestset "server" begin
            include("server/test_server.jl")
        end
        @time @safetestset "server_late_expose" begin
            include("server/test_server_late_expose.jl")
        end
        @time @safetestset "broker_server_timeout" begin
            include("server/test_broker_server_timeout.jl")
        end
        @time @safetestset "loopback" begin
            include("server/test_loopback.jl")
        end
        @time @safetestset "register_server" begin
            include("server/test_register_server.jl")
        end
    end
    if GROUP == "all" || GROUP == "ack"
        @time @safetestset "qos0" begin
            include("ack/test_qos0.jl")
        end
        @time @safetestset "qos1" begin
            include("ack/test_qos1.jl")
        end
        @time @safetestset "qos2" begin
            include("ack/test_qos2.jl")
        end
        @time @safetestset "simple_ack" begin
            include("ack/test_simple_ack.jl")
        end
        @time @safetestset "ws_ack" begin
            include("ack/test_ws_ack.jl")
        end
        @time @safetestset "zmq_ack" begin
            include("ack/test_zmq_ack.jl")
        end
        @time @safetestset "publish_side" begin
            include("ack/test_publish_side.jl")
        end
        @time @safetestset "at_least_once" begin
            include("ack/test_at_least_once.jl")
        end
        @time @safetestset "exactly_once" begin
            include("ack/test_exactly_once.jl")
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
        @time @safetestset "msg_from" begin
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
    if GROUP == "all" || GROUP == "security"
        @time @safetestset "register_authenticated" begin
            include("security/test_register_authenticated.jl")
        end
        @time @safetestset "connect_authenticated" begin
            include("security/test_connect_authenticated.jl")
        end
        @time @safetestset "connect_anonymous" begin
            include("security/test_connect_anonymous.jl")
        end
        @time @safetestset "test_challenge_not_sent" begin
            include("security/test_challenge_not_sent.jl")
        end
        @time @safetestset "test_inquiry_not_sent" begin
            include("security/test_inquiry_not_sent.jl")
        end
        @time @safetestset "unauth_command" begin
            include("security/test_unauth_command.jl")
        end
        @time @safetestset "auth_failed" begin
            include("security/test_auth_failed.jl")
        end
        @time @safetestset "unregister_authenticated" begin
            include("security/test_unregister_authenticated.jl")
        end
        #        @time @safetestset "nocommands_authenticated" begin
        #            include("security/test_nocommands_authenticated.jl")
        #        end
    end
    if GROUP == "all" || GROUP == "future"
        @time @safetestset "future" begin
            include("future/test_future.jl")
        end
    end
    if GROUP == "all" || GROUP == "api"
        @time @safetestset "supervised_api" begin
            include("api/test_supervised_api.jl")
        end
        @time @safetestset "component_tomany" begin
            include("api/test_component_tomany.jl")
        end
        @time @safetestset "component" begin
            include("api/test_component.jl")
        end
        @time @safetestset "simple_publish" begin
            include("api/test_simple_publish.jl")
        end
        @time @safetestset "simple_rpc" begin
            include("api/test_simple_rpc.jl")
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
        @time @safetestset "subscribe" begin
            include("api/test_subscribe.jl")
        end
        @time @safetestset "subscribe_glob" begin
            include("api/test_subscribe_glob.jl")
        end
        @time @safetestset "reactive" begin
            include("api/test_reactive.jl")
        end
    end
    if GROUP == "all" || GROUP == "auth"
        @time @safetestset "register" begin
            include("auth/test_register.jl")
        end
        @time @safetestset "register_multitenancy" begin
            include("auth/test_register_multitenancy.jl")
        end
        @time @safetestset "register_ecdsa" begin
            include("auth/test_register_ecdsa.jl")
        end
        @time @safetestset "register_multiple_users" begin
            include("auth/test_register_multiple_users.jl")
        end
        @time @safetestset "register_disabled_user" begin
            include("auth/test_register_disabled_user.jl")
        end
        @time @safetestset "login_failure" begin
            include("auth/test_login_failure.jl")
        end
        @time @safetestset "wrong_secret" begin
            include("auth/test_wrong_secret.jl")
        end
        @time @safetestset "no_http_ca_bundle" begin
            include("auth/test_no_http_ca_bundle.jl")
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
        @time @safetestset "unpark" begin
            include("park/test_unpark.jl")
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
        @time @safetestset "tcp" begin
            include("tcp/test_tcp.jl")
        end
        @time @safetestset "wrong_response" begin
            include("tcp/test_wrong_response.jl")
        end
    end
    if GROUP == "all" || GROUP == "broker_plugin"
        @time @safetestset "policy" begin
            include("broker_plugin/test_policy.jl")
        end
        @time @safetestset "error_plugin" begin
            include("broker_plugin/test_error_plugin.jl")
        end
        @time @safetestset "multiplexer" begin
            include("broker_plugin/test_multiplexer.jl")
        end
        @time @safetestset "multiplexer_fault" begin
            include("broker_plugin/test_multiplexer_fault.jl")
        end
        @time @safetestset "broker_plugin" begin
            include("broker_plugin/test_plugin.jl")
        end
        @time @safetestset "publish_interceptor" begin
            include("broker_plugin/test_publish_interceptor.jl")
        end
    end
    if GROUP == "all" || GROUP == "repl"
        @time @safetestset "repl" begin
            include("repl/test_repl.jl")
        end
        @time @safetestset "server_repl" begin
            include("repl/test_server_repl.jl")
        end
    end
    if GROUP == "all" || GROUP == "errors"
        @time @safetestset "invalid_mode" begin
            include("errors/test_invalid_mode.jl")
        end
        @time @safetestset "unknown_broker_msg" begin
            include("errors/test_unknown_broker_msg.jl")
        end
        @time @safetestset "authenticate_timeout" begin
            include("errors/test_authenticate_timeout.jl")
        end
        @time @safetestset "rpc_timeout" begin
            include("errors/test_rpc_timeout.jl")
        end
        @time @safetestset "router_zmq_message" begin
            include("errors/test_router_zmq_message.jl")
        end
        @time @safetestset "dealer_zmq_message" begin
            include("errors/test_dealer_zmq_message.jl")
        end
        @time @safetestset "invalid_states" begin
            include("errors/test_invalid_states.jl")
        end
        @time @safetestset "rembus_errors" begin
            include("errors/test_rembus_errors.jl")
        end
        @time @safetestset "serve_ws_error" begin
            include("errors/test_serve_ws_error.jl")
        end
        @time @safetestset "serve_zmq_error" begin
            include("errors/test_serve_zmq_error.jl")
        end
        @time @safetestset "identity_empty_string" begin
            include("errors/test_identity_empty_string.jl")
        end
        @time @safetestset "connection_error" begin
            include("errors/test_connection_error.jl")
        end
        @time @safetestset "transport_send_error" begin
            include("errors/test_transport_send_error.jl")
        end
    end
end
