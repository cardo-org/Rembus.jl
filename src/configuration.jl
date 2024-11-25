macro showerror(e)
    quote
        if CONFIG.stacktrace
            showerror(stdout, $(esc(e)), catch_backtrace())
        end
    end
end

# Get the default name for component, mainly used by macro APIs
function component_id(cfg)
    url = get(cfg, "cid", get(ENV, "REMBUS_CID", ""))
    return RbURL(url)
end

function default_rembus_dir()
    if Sys.iswindows()
        home = get(ENV, "USERPROFILE", ".")
    else
        home = get(ENV, "HOME", ".")
    end
    return joinpath(home, ".config", "rembus")
end

rembus_dir() = CONFIG.rembus_dir

function rembus_dir!(new_dir::AbstractString)
    old_dir = CONFIG.rembus_dir
    CONFIG.rembus_dir = new_dir
    return old_dir
end

mutable struct Settings
    zmq_ping_interval::Float32
    ws_ping_interval::Float32
    rembus_dir::String
    log_destination::String
    log_level::String
    overwrite_connection::Bool
    stacktrace   # log stacktrace on error
    metering     # log in and out messages
    rawdump      # log in and out raw bytes
    cid::RbURL
    connection_retry_period::Float32 # seconds between reconnection attempts
    broker_plugin::Union{Nothing,Module}
    save_messages::Bool
    db_max_messages::UInt
    connection_mode::ConnectionMode
    Settings() = begin
        zmq_ping_interval = 0
        ws_ping_interval = 0
        rdir = default_rembus_dir()
        log_destination = "stdout"
        log_level = TRACE_INFO
        overwrite_connection = true
        stacktrace = false
        metering = false
        rawdump = false
        cid = RbURL()
        connection_retry_period = 2.0
        db_max_messages = parse(UInt, REMBUS_DB_MAX_SIZE)
        new(zmq_ping_interval, ws_ping_interval, rdir, log_destination, log_level,
            overwrite_connection, stacktrace, metering, rawdump, cid,
            connection_retry_period, nothing, true, db_max_messages, anonymous)
    end
end

function string_to_enum(connection_mode)
    if connection_mode == "anonymous"
        return anonymous
    elseif connection_mode == "authenticated"
        return authenticated
    else
        error("invalid connection_mode [$connection_mode]")
    end
end

function setup(setting)
    cfg = get(Base.get_preferences(), "Rembus", Dict())

    setting.zmq_ping_interval = get(cfg, "zmq_ping_interval",
        parse(Float32, get(ENV, "REMBUS_ZMQ_PING_INTERVAL", "10")))

    setting.ws_ping_interval = get(cfg, "ws_ping_interval",
        parse(Float32, get(ENV, "REMBUS_WS_PING_INTERVAL", "0")))

    setting.rembus_dir = get(cfg, "rembus_dir", get(ENV, "REMBUS_DIR", rembus_dir()))
    setting.log_destination = get(cfg, "log_destination", get(ENV, "BROKER_LOG", "stdout"))
    setting.overwrite_connection = get(cfg, "overwrite_connection", true)
    setting.stacktrace = get(cfg, "stacktrace", false)
    setting.metering = get(cfg, "metering", false)
    setting.rawdump = get(cfg, "rawdump", false)
    setting.cid = component_id(cfg)
    setting.connection_retry_period = get(cfg, "connection_retry_period", 2.0)

    connection_mode = get(cfg, "connection_mode", "anonymous")
    setting.connection_mode = string_to_enum(connection_mode)

    setting.db_max_messages = get(
        cfg,
        "db_max_messages",
        parse(UInt, get(ENV, "REMBUS_DB_MAX_SIZE", REMBUS_DB_MAX_SIZE))
    )
    setting.save_messages = get(
        cfg,
        "save_messages",
        parse(Bool, get(ENV, "REMBUS_SAVE_MESSAGES", "true"))
    )

    if haskey(ENV, "REMBUS_DEBUG")
        setting.log_level = TRACE_INFO
        if ENV["REMBUS_DEBUG"] == "1"
            setting.log_level = TRACE_DEBUG
        end
    else
        setting.log_level = get(cfg, "log_level", TRACE_INFO)
    end

    return nothing
end

const CONFIG = Settings()
