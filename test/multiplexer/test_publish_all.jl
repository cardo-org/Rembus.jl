include("../utils.jl")


mytopic(ctx, rb, data) = @info "[$rb]: received $data"

function start_servers()
    for port in [7000, 7001]
        rb = server(ws=port)
        expose(rb, myservice)
    end
end

function run()

    start_servers()

    set_balancer("all")
    rb = connect(["ws://:7000", "ws://:6001", "ws://:7001"])

    publish(rb, "mytopic", "hello")

    close(rb)
    set_balancer("first_up")
end


@info "[test_publish_all] start"
run()
@info "[test_publish_all] stop"

shutdown()
