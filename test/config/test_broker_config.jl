include("../utils.jl")

using JSON3

subscribers = Dict(
    "cid1" => Dict("topic1" => true),
    "cid2" => Dict("topic1" => true, "topic2" => false)
)

exposers = Dict(
    "topic1" => ["cid1", "cid2"],
    "topic2" => ["cid1", "cid2", "cid3"]
)

topic_auth = Dict(
    "topic1" => ["cid1"]
)

function set_exposers()
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "exposers.json")
    open(fn, "w") do io
        write(io, JSON3.write(exposers))
    end

end

function set_subscribers()
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "subscribers.json")
    open(fn, "w") do io
        write(io, JSON3.write(subscribers))
    end

end

function set_topic_auth()
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "topic_auth.json")
    open(fn, "w") do io
        write(io, JSON3.write(topic_auth))
    end
end

function setup(admin)
    bdir = Rembus.broker_dir(BROKER_NAME)
    mkpath(bdir)

    @info "broker_dir:$bdir - ($(pwd())) $(isdir(bdir))"
    fn = joinpath(bdir, "admins.json")
    @info "setting admin: $fn"
    open(fn, "w") do io
        write(io, JSON3.write(Set([admin])))
    end
    @info "admins.json setup done"
    set_exposers()
    set_subscribers()
    set_topic_auth()
end

function teardown()
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([])))
    end
end

function run()
    rb = connect(admin)
    try
        cfg = Rembus.broker_config(rb)
        @info "broker config: $cfg"
        @test keys(cfg["exposers"]) == keys(exposers)
    catch e
        @error "[test_broker_config] error: $e"
        showerror(stdout, e, catch_backtrace())
        @test false
    finally
        close(rb)
    end
end

admin = "admin"
setup() = setup(admin)
execute(run, "test_broker_config", setup=setup, reset=false)
teardown()
