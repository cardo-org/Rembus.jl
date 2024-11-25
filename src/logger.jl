struct RembusLogger <: AbstractLogger
    io::IO
end

function repl_log()
    ConsoleLogger(stdout, Info, meta_formatter=repl_metafmt) |> global_logger
end

function repl_metafmt(level::LogLevel, _module, group, id, file, line)
    @nospecialize
    color = Logging.default_logcolor(level)
    prefix = string(level == Warn ? "Warning" : string(level), ':')
    suffix::String = ""
    return color, prefix, suffix
end

function logging()
    if CONFIG.log_destination === "stdout"
        rlogger = RembusLogger(stdout)
        rlogger |> global_logger
    else
        rlogger = RembusLogger(open(CONFIG.log_destination, "a+"))
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
    elseif CONFIG.log_level == "error"
        level >= Logging.Warn
    elseif CONFIG.log_level == "debug" && (_module === Rembus || _module === Visor)
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
