#=
SPDX-License-Identifier: AGPL-3.0-only

Copyright (C) 2024  Attilio Donà attilio.dona@gmail.com
Copyright (C) 2024  Claudio Carraro carraro.claudio@gmail.com
=#

struct RembusLogger <: AbstractLogger
    io::IO
    groups::Vector{Symbol}
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

function logging(; debug=[])
    groups = []
    for item in debug
        isa(item, Module) && push!(CONFIG.debug_modules, item)
        isa(item, Symbol) && push!(groups, item)
    end

    if CONFIG.log === "stdout"
        RembusLogger(stdout, groups) |> global_logger
    else
        RembusLogger(open(CONFIG.log, "a+"), groups) |> global_logger
    end

    return nothing
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
    else
        level >= Logging.Info || group in logger.groups || _module in CONFIG.debug_modules
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
