using ArgParse

function command_line(default_name="broker")
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--name", "-n"
        help = "broker name"
        default = default_name
        arg_type = String
        "--reset", "-x"
        help = "factory reset, clean up broker configuration"
        action = :store_true
        "--secure", "-s"
        help = "accept wss and tls connections"
        action = :store_true
        "--authenticated", "-a"
        help = "only authenticated components allowed"
        action = :store_true
        "--http", "-p"
        help = "accept HTTP clients on port HTTP"
        arg_type = UInt16
        "--prometheus", "-m"
        help = "prometheus exposer port"
        arg_type = UInt16
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

function brokerd()::Cint
    args = command_line()
    name = args["name"]
    if args["reset"]
        Rembus.broker_reset(name)
    end

    if args["debug"]
        Rembus.debug!()
    elseif args["info"]
        Rembus.info!()
    end

    try
        wait(Rembus.broker(
            name=name,
            ws=args["ws"],
            tcp=args["tcp"],
            zmq=args["zmq"],
            prometheus=args["prometheus"],
            secure=args["secure"],
            http=args["http"],
            authenticated=args["authenticated"],
            policy=args["policy"]
        ))
    catch e
        println(e)
    end
    return 0
end
