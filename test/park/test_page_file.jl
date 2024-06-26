include("../utils.jl")

mutable struct Ctx
    count::UInt
end

const MESSAGES = 100000

mytopic(ctx) = ctx.count += 1
myservice(x) = x

function first_run()
    sub = tryconnect("mysub")
    subscribe(sub, mytopic)

    # add an exposer to the impls table
    # to be loaded in the second steps
    expose(sub, myservice)
    close(sub)

    pub = connect()
    for i in 1:MESSAGES
        publish(pub, "mytopic")
    end
    sleep(15)
    close(pub)
end

function second_run()
    ctx = Ctx(0)
    sub = connect("mysub")
    shared(sub, ctx)
    subscribe(sub, mytopic, true)
    reactive(sub)
    sleep(5)
    @info "messages:$(Int(ctx.count))"
    count = 0
    while (ctx.count < MESSAGES) && (count < 10)
        sleep(1)
        count += 1
    end
    close(sub)
    @info "total messages:$(Int(ctx.count))"
    @test ctx.count == MESSAGES
end

execute(first_run, "test_page_file::1")
sleep(3)
execute(second_run, "test_page_file::2", reset=false)
