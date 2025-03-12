
function dumperror(e)
    if get(getcfg(), "stacktrace", false)
        showerror(stdout, e, catch_backtrace())
    end
end

getcfg() = get(Base.get_preferences(), "Rembus", Dict())

function cid!(url)
    set_preferences!(Rembus, "cid" => url, force=true)
end

cid() = get(getcfg(), "cid", get(ENV, "REMBUS_CID", ""))

function default_rembus_dir()
    if Sys.iswindows()
        home = get(ENV, "USERPROFILE", ".")
    else
        home = get(ENV, "HOME", ".")
    end
    return joinpath(home, ".config", "rembus")
end

function rembus_dir()
    cfg = getcfg()
    get(cfg, "rembus_dir", get(ENV, "REMBUS_DIR", default_rembus_dir()))
end

function rembus_dir!(new_dir::AbstractString)
    set_preferences!(Rembus, "rembus_dir" => new_dir, force=true)
end

function init_log(level=nothing)
    if !haskey(ENV, "JULIA_DEBUG")
        isnothing(level) ? logging("warn") : logging(level)
    end
end

debug!() = init_log("debug")
info!() = init_log("info")
warn!() = init_log("warn")
error!() = init_log("error")

anonymous!() = set_preferences!(
    Rembus, "connection_mode" => string(anonymous), force=true
)

authenticated!() = set_preferences!(
    Rembus, "connection_mode" => string(authenticated), force=true
)

function string_to_enum(connection_mode)
    if connection_mode == "anonymous"
        return anonymous
    elseif connection_mode == "authenticated"
        return authenticated
    else
        error("invalid connection_mode [$connection_mode]")
    end
end

function request_timeout()
    cfg = getcfg()
    get(cfg, "request_timeout", get(ENV, "REMBUS_TIMEOUT", "5"))
end

function request_timeout!(newval)
    set_preferences!(Rembus, "request_timeout" => newval, force=true)
end

function challenge_timeout!(newval)
    set_preferences!(Rembus, "challenge_timeout" => newval, force=true)
end

function ack_timeout!(newval)
    set_preferences!(Rembus, "ack_timeout" => newval, force=true)
end

function ws_ping_interval!(newval)
    set_preferences!(Rembus, "ws_ping_interval" => newval, force=true)
end

function zmq_ping_interval!(newval)
    set_preferences!(Rembus, "zmq_ping_interval" => newval, force=true)
end
