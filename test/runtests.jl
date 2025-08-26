using Rembus
using SafeTestsets
using Test
using TestItemRunner

const GROUP = get(ENV, "GROUP", "all")

@run_package_tests

@testset "Rembus" begin
    if GROUP == "all" || GROUP == "unit"
        @time @safetestset "misc" begin
            include("unit/test_misc.jl")
        end
        @time @safetestset "policies_cases" begin
            include("unit/test_policies_cases.jl")
        end
        @time @safetestset "cbor" begin
            include("unit/test_cbor.jl")
        end
    end
    if GROUP == "all" || GROUP == "twin"
        @time @safetestset "socket_send" begin
            include("twin/test_socket_send.jl")
        end
    end
    if GROUP == "all" || GROUP == "broker"
        @time @safetestset "anonymous_twin" begin
            include("broker/test_anonymous_twin.jl")
        end
        @time @safetestset "overwrite_connection" begin
            include("broker/test_overwrite_connection.jl")
        end
        @time @safetestset "plugin" begin
            include("broker/test_plugin.jl")
        end
        @time @safetestset "data_at_rest" begin
            include("broker/test_data_at_rest.jl")
        end
        @time @safetestset "ha_rpc" begin
            include("broker/test_ha_rpc.jl")
        end
        @time @safetestset "ha_pub" begin
            include("broker/test_ha_pub.jl")
        end
        @time @safetestset "ha_setup" begin
            include("broker/test_ha_setup.jl")
        end
        @time @safetestset "simo" begin
            include("broker/test_simo.jl")
        end
        @time @safetestset "setup" begin
            include("broker/test_setup.jl")
        end
    end
    if GROUP == "all" || GROUP == "json-rpc"
        @time @safetestset "jsonrpc" begin
            include("json-rpc/test_jsonrpc.jl")
        end
        @time @safetestset "jsonrpc_http" begin
            include("json-rpc/test_jsonrpc_http.jl")
        end
        @time @safetestset "jsonrpc_http_namedargs" begin
            include("json-rpc/test_jsonrpc_http_namedargs.jl")
        end
        @time @safetestset "jsonrpc_pubsub" begin
            include("json-rpc/test_jsonrpc_pubsub.jl")
        end
        @time @safetestset "jsonrpc_register" begin
            include("json-rpc/test_jsonrpc_register.jl")
        end
    end
    if GROUP == "all" || GROUP == "api"
        @time @safetestset "ifdown_block" begin
            include("api/test_ifdown_block.jl")
        end
        @time @safetestset "policies" begin
            include("api/test_policies.jl")
        end
        @time @safetestset "pool" begin
            include("api/test_pool.jl")
        end
        @time @safetestset "pool_component" begin
            include("api/test_pool_component.jl")
        end
        @time @safetestset "component" begin
            include("api/test_component.jl")
        end
        @time @safetestset "dataframe" begin
            include("api/test_dataframe.jl")
        end
        @time @safetestset "publish" begin
            include("api/test_publish.jl")
        end
        @time @safetestset "publish_tenants" begin
            include("api/test_publish_tenants.jl")
        end
        @time @safetestset "rpc_tenants" begin
            include("api/test_rpc_tenants.jl")
        end
        @time @safetestset "expose" begin
            include("api/test_expose.jl")
        end
        @time @safetestset "publish_qos2" begin
            include("api/test_publish_qos2.jl")
        end
        @time @safetestset "server" begin
            include("api/test_server.jl")
        end
        @time @safetestset "types" begin
            include("api/test_types.jl")
        end
        @time @safetestset "subscribe_glob" begin
            include("api/test_subscribe_glob.jl")
        end
        @time @safetestset "request" begin
            include("api/test_request.jl")
        end
        @time @safetestset "macros" begin
            include("api/test_macros.jl")
        end
        @time @safetestset "server" begin
            include("api/test_server.jl")
        end
    end
    if GROUP == "all" || GROUP == "errors"
        @time @safetestset "unexpected_messages" begin
            include("errors/test_unexpected_messages.jl")
        end
        @time @safetestset "connect_errors" begin
            include("errors/test_connect_errors.jl")
        end
        @time @safetestset "wrong_message" begin
            include("errors/test_wrong_message.jl")
        end
        @time @safetestset "dealer_zmq" begin
            include("errors/test_dealer_zmq.jl")
        end
        @time @safetestset "router_zmq" begin
            include("errors/test_router_zmq.jl")
        end
    end
    if GROUP == "all" || GROUP == "private"
        @time @safetestset "private_topic" begin
            include("private/test_private_topic.jl")
        end
    end
    if GROUP == "all" || GROUP == "promethues"
        @time @safetestset "prometheus" begin
            include("prometheus/test_prometheus.jl")
        end
    end
    if GROUP == "all" || GROUP == "security"
        @time @safetestset "challenge" begin
            include("security/test_challenge.jl")
        end
        @time @safetestset "await_attestation" begin
            include("security/test_await_attestation.jl")
        end
        @time @safetestset "authenticated" begin
            include("security/test_authenticated.jl")
        end
        @time @safetestset "register" begin
            include("security/test_register.jl")
        end
        @time @safetestset "register_authenticated" begin
            include("security/test_register_authenticated.jl")
        end
        @time @safetestset "register_multiple_users" begin
            include("security/test_register_multiple_users.jl")
        end
        @time @safetestset "register_multitenancy" begin
            include("security/test_register_multitenancy.jl")
        end
    end
    if GROUP == "all" || GROUP == "tcp"
        @time @safetestset "tcp_connect" begin
            include("tcp/test_tcp_connect.jl")
        end
    end
    if GROUP == "all" || GROUP == "zmq"
        @time @safetestset "zmq_connect" begin
            include("zmq/test_zmq_connect.jl")
        end
        @time @safetestset "resend_attestate" begin
            include("zmq/test_zmq_resend_attestate.jl")
        end
    end
    if GROUP == "all" || GROUP == "ws"
        @time @safetestset "keep_alive" begin
            include("ws/test_keep_alive.jl")
        end
        @time @safetestset "reconnect" begin
            include("ws/test_reconnect.jl")
        end
    end
    if GROUP == "all" || GROUP == "http"
        @time @safetestset "http_authenticated" begin
            include("http/test_http_authenticated.jl")
        end
        @time @safetestset "http" begin
            include("http/test_http.jl")
        end
        @time @safetestset "https" begin
            include("http/test_https.jl")
        end
        @time @safetestset "http_admin" begin
            include("http/test_http_admin.jl")
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
    if GROUP == "all" || GROUP == "ack"
        @time @safetestset "already_received" begin
            include("ack/test_already_received.jl")
        end
        @time @safetestset "saved_messages" begin
            include("ack/test_saved_messages.jl")
        end
        @time @safetestset "ack_timeout" begin
            include("ack/test_ack_timeout.jl")
        end
    end
end
