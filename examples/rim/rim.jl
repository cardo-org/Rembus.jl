using ArgParse
using Rembus
using DuckDB

const APP_TABLE = """
CREATE TABLE IF NOT EXISTS
app(name TEXT PRIMARY KEY, version TEXT NOT NULL, cksum TEXT NOT NULL,
status TEXT, type TEXT DEFAULT 'julia')
"""

struct Application
    name::String
    version::String
    status::String
end

struct Rim
    rb::Rembus.Twin
    db::DuckDB.DB
    appdir::String
    apps::Dict{String,Application}
    function Rim(rb, appdir)
        db = DuckDB.DB("rim.db")
        DuckDB.execute(db, APP_TABLE)

        return new(rb, db, appdir, Dict())
    end
end

app_module(name) = name

app_path(hv, name) = joinpath(@__DIR__, hv.appdir, name)

function command_line(app_dir="apps")
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--appdir", "-a"
        help = "apps dir"
        default = app_dir
        arg_type = String
        "--edge", "-e"
        help = "edge url"
        arg_type = String
        "--ws", "-w"
        help = "accept WebSocket clients on port WS"
        arg_type = UInt16
        "--tcp", "-t"
        help = "accept tcp clients on port TCP"
        arg_type = UInt16
        "--zmq", "-z"
        help = "accept zmq clients on port ZMQ"
        arg_type = UInt16
        "--policy", "-r"
        help = "set the broker routing policy: first_up, round_robin, less_busy"
        default = "first_up"
        arg_type = String
        "--debug", "-d"
        help = "enable debug logs"
        action = :store_true
        "--info", "-i"
        help = "enable info logs"
        action = :store_true
    end
    return parse_args(s)
end

function cksum(hv, appname)
    # fn = joinpath(@__DIR__, hv.appdir, appname)
    fn = app_path(hv, appname)
    out = readchomp(`cksum $fn`)
    (value, _, _) = split(out)
    return value
end


function install_app(hv::Rim, rb, name, version, type, content)
    @info "[$hv] installing $name:$version"

    mod = app_module(name)
    #write(joinpath(@__DIR__, hv.appdir, mod), content)
    write(app_path(hv, mod), content)

    if type == "julia"
        enable(hv, mod)
    end

    csum = cksum(hv, mod)

    DuckDB.execute(
        hv.db,
        """
        INSERT INTO app (name, version, cksum, status, type)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            cksum = excluded.cksum,
            version = excluded.version,
            status = excluded.status,
            type = excluded.type,
        """,
        (name, version, csum, "on", type))

    return "$name ok"
end

function enable_app(hv::Rim, rb, name)
    mod = app_module(name)
    enable(hv, mod)
end

function disable_app(hv::Rim, rb, name)
    mod = app_module(name)
    disable(hv, mod)
end


function rim()
    args = command_line()

    if !isnothing(get(args, "edge", nothing))
        @info "args: $args"
        rb = component(args["edge"])
    else
        rb = component()
    end

    hv = Rim(rb, args["appdir"])

    inject(rb, hv)
    expose(rb, install_app)
    expose(rb, enable_app)
    expose(rb, disable_app)

    # start the apps
    start_apps(hv, args)

    #    # Create a fresh module for isolation
    #    mod = Module(Symbol("App_", fn))
    #    Base.include(mod, app)
    #    Core.eval(mod, :(start($rb)))  # call start(rb) inside that module
    return rb
end

function enable(hv, name)
    if endswith(name, ".jl")
        fn = app_path(hv, name)
        mod = Module(Symbol(basename(fn)))
        @info "created module: $mod"
        Base.include(mod, fn)
        Core.eval(mod, :(start($(hv.rb))))
    end
end

function disable(hv, name)
    if endswith(name, ".jl")
        fn = app_path(hv, name)
        mod = Module(Symbol(basename(fn)))
        @info "created module: $mod"
        Base.include(mod, fn)
        try
            Core.eval(mod, :(stop($(hv.rb))))
        catch e
            if isa(e, UndefVarError)
                error("$name: stop method not implemented")
            else
                rethrow()
            end
        end
    end
end

function run_external(hv, rb, topic, args)
    @info "running [$topic $args]"

    pth = app_path(hv, "$topic.sh")

    out = String(readchomp(`$pth $args`))
    return out
end

function start_apps(hv, args)
    app_dir = args["appdir"]
    rb = hv.rb
    @info "app_dir: $(@__DIR__) $app_dir"
    for fn in readdir(joinpath(@__DIR__, app_dir))
        @info "!! loading $fn"

        if endswith(fn, ".jl")
            enable(hv, fn)
            #            # Create a fresh module for isolation
            #            mod = Module(Symbol(basename(fn)))
            #            @info "created module: $mod"
            #
            #            pth = app_path(hv, fn)
            #            Base.include(mod, pth)
            #            Core.eval(mod, :(start($rb)))  # call start(rb) inside that module
        else
            (topic, _) = splitext(fn)
            @info "exposing $topic"
            expose(rb, topic, (hv, rb, args) -> run_external(hv, rb, topic, args))
        end
    end
end


function main()
    rb = rim()
    wait(rb)
end

main()
