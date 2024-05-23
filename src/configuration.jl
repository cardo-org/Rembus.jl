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
    return startswith(uri.path, "/") ? uri.path[2:end] : uri.path
end

mutable struct Settings
    zmq_ping_interval::Float32
    ws_ping_interval::Float32
    balancer::String
    home::String
    db::String
    log::String
    debug::Bool
    overwrite_connection::Bool
    stacktrace   # log stacktrace on error
    metering     # log in and out messages
    rawdump      # log in and out raw bytes
    cid::String  # rembus default component cid
    connection_retry_period::Float32 # seconds between reconnection attempts
    broker_plugin::Union{Nothing,Module}
    broker_ctx::Any
    page_size::UInt
    Settings(rootdir=nothing) = begin
        if rootdir === nothing
            if Sys.iswindows()
                home = get(ENV, "USERPROFILE", ".")
            else
                home = get(ENV, "HOME", ".")
            end
        else
            home = rootdir
        end

        zmq_ping_interval = 0
        ws_ping_interval = 0
        balancer = "first_up"
        db = joinpath(home, ".config", "caronte")
        log = "stdout"
        overwrite_connection = true
        stacktrace = false
        metering = false
        rawdump = false
        cid = DEFAULT_APP_NAME
        connection_retry_period = 2.0
        debug = false
        page_size = get(ENV, "REMBUS_PAGE_SIZE", REMBUS_PAGE_SIZE)
        new(zmq_ping_interval, ws_ping_interval, balancer, home, db, log, debug,
            overwrite_connection, stacktrace, metering, rawdump, cid,
            connection_retry_period, nothing, nothing, page_size)
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
    cfg = get(Base.get_preferences(), "Rembus", Dict())

    setting.zmq_ping_interval = get(cfg, "zmq_ping_interval",
        parse(Float32, get(ENV, "REMBUS_ZMQ_PING_INTERVAL", "10")))

    setting.ws_ping_interval = get(cfg, "ws_ping_interval",
        parse(Float32, get(ENV, "REMBUS_WS_PING_INTERVAL", "120")))

    setting.db = get(cfg, "db", get(ENV, "BROKER_DIR", setting.db))
    setting.log = get(cfg, "log", get(ENV, "BROKER_LOG", "stdout"))
    setting.overwrite_connection = get(cfg, "overwrite_connection", true)
    setting.stacktrace = get(cfg, "stacktrace", false)
    setting.metering = get(cfg, "metering", false)
    setting.rawdump = get(cfg, "rawdump", false)
    setting.cid = component_id(cfg)
    setting.connection_retry_period = get(cfg, "connection_retry_period", 2.0)
    setting.page_size = get(cfg, "page_size", setting.page_size)

    if haskey(ENV, "REMBUS_DEBUG")
        setting.debug = false
        if ENV["REMBUS_DEBUG"] == "1"
            setting.debug = true
        end
    else
        setting.debug = get(cfg, "debug", false)
    end
    balancer = get(cfg, "balancer", get(ENV, "BROKER_BALANCER", "first_up"))
    set_balancer(setting, balancer)

    return nothing
end

const Rembus.CONFIG = Rembus.Settings()
