struct RembusLogger <: AbstractLogger
    io::IO
    level::String
end

function repl_metafmt(level::LogLevel, _module, group, id, file, line)
    @nospecialize
    color = Logging.default_logcolor(level)
    prefix = string(level == Warn ? "Warning" : string(level), ':')
    suffix::String = ""
    return color, prefix, suffix
end

function logging(level)
    log_destination = get(ENV, "BROKER_LOG", "stdout")
    if log_destination === "stdout"
        rlogger = RembusLogger(stdout, level)
        rlogger |> global_logger
    else
        rlogger = RembusLogger(open(log_destination, "a+"), level)
        rlogger |> global_logger
    end

    return rlogger
end

function Logging.min_enabled_level(logger::RembusLogger)
    Logging.Debug
end

function Logging.shouldlog(
    logger::RembusLogger,
    level,
    _module,
    group,
    id
)
    if _module === HTTP.Servers
        level >= Logging.Warn
    elseif logger.level == "error"
        level >= Logging.Error
    elseif logger.level == "warn"
        level >= Logging.Warn
    elseif logger.level == "debug" && (_module === Rembus || _module === Visor)
        level >= Logging.Debug
    else
        level >= Logging.Info
    end
end

function Logging.handle_message(
    logger::RembusLogger,
    level,
    message,
    _module,
    group,
    id,
    file,
    line;
    kwargs...
)
    println(logger.io, "[$(now())][$_module][$(Threads.threadid())][$level] $message")
    flush(logger.io)
end
