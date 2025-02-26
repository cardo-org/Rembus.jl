include("../utils.jl")

foo() = "ciao mondo"

function run()

    Rembus.cid!("policies_component")

    server = connect("server")

    nodes = ["a", "b", "c"]
    for policy in [:round_robin, :less_busy]
        rb = component(nodes, policy=policy)
        @info "[$rb] socket: $(rb.socket)"
        expose(server, foo)

        # Disconnect the component b
        proc_b = from("policies_component.twins.b")
        if !isnothing(proc_b)
            b = proc_b.args[1]
            shutdown(b.process)
            #sleep(0.5)
        end

        for round in 1:6
            result = rpc(rb, "foo")
            @test result == foo()
        end

        close(rb)
    end
end

execute(run, "policies")
