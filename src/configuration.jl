#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

const DEFAULT_APP_NAME = "rembus"

macro showerror(e)
    quote
        if CONFIG.stacktrace
            showerror(stdout, $(esc(e)), catch_backtrace())
        end
    end
end

function component_id(cfg)
    url = get(cfg, "cid", get(ENV, "REMBUS_CID", DEFAULT_APP_NAME))
    uri = URI(url)
    proto = uri.scheme
    if proto == ""
        name = uri.path
    else
        name = uri.host
    end
    name
end

mutable struct Settings
    zmq_ping_interval::Float32
    ws_ping_interval::Float32
    balancer::String
    db::String
    log::String
    debug_modules::Vector{Module}
    overwrite_connection::Bool
    stacktrace   # log stacktrace on error
    metering     # log in and out messages
    rawdump      # log in and out raw bytes
    cid::String  # rembus default component cid
    connection_retry_period::Float32 # seconds between reconnection attempts
    broker_plugin::Union{Nothing,Module}
    broker_ctx::Any
    Settings() = begin
        zmq_ping_interval = 0
        ws_ping_interval = 0
        balancer = "first_up"
        db = joinpath(get(ENV, "HOME", "."), "caronte")
        log = "stdout"
        overwrite_connection = true
        stacktrace = false
        metering = false
        rawdump = false
        cid = DEFAULT_APP_NAME
        connection_retry_period = 2.0
        debug_modules = []

        new(zmq_ping_interval, ws_ping_interval, balancer, db, log, debug_modules,
            overwrite_connection, stacktrace, metering, rawdump, cid,
            connection_retry_period, nothing, nothing)
    end
end

set_balancer(policy::AbstractString) = set_balancer(CONFIG, policy)

function set_balancer(setting, policy)
    #balancer = get(cfg, "balancer", get(ENV, "BROKER_BALANCER", "first_up"))
    if !(policy in ["first_up", "less_busy", "round_robin"])
        error("wrong balancer, must be one of first_up, less_busy, round_robin")
    end
    setting.balancer = policy

    return nothing
end

function setup(setting)
    home = get(ENV, "HOME", ".")
    cfg = get(Base.get_preferences(), "Rembus", Dict())

    setting.zmq_ping_interval = get(cfg, "zmq_ping_interval",
        parse(Float32, get(ENV, "REMBUS_ZMQ_PING_INTERVAL", "10")))

    setting.ws_ping_interval = get(cfg, "ws_ping_interval",
        parse(Float32, get(ENV, "REMBUS_WS_PING_INTERVAL", "120")))

    setting.db = get(cfg, "db", get(ENV, "REMBUS_DB", joinpath(home, "caronte")))
    setting.log = get(cfg, "log", get(ENV, "BROKER_LOG", "stdout"))
    setting.overwrite_connection = get(cfg, "overwrite_connection", true)
    setting.stacktrace = get(cfg, "stacktrace", false)
    setting.metering = get(cfg, "metering", false)
    setting.rawdump = get(cfg, "rawdump", false)
    setting.cid = component_id(cfg)
    setting.connection_retry_period = get(cfg, "connection_retry_period", 2.0)

    if get(ENV, "REMBUS_DEBUG", "0") == "1"
        setting.debug_modules = [Rembus, Visor]
    else
        setting.debug_modules = []
    end

    balancer = get(cfg, "balancer", get(ENV, "BROKER_BALANCER", "first_up"))
    set_balancer(setting, balancer)

    return nothing
end

context() = CONFIG.broker_ctx