include("../utils.jl")

using JSON3

function teardown()
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "admins.json")
    open(fn, "w") do io
        write(io, JSON3.write(Set([])))
    end
end

function set_topic_auth()
    topic_auth = Dict(
        "topic1" => ["cid1"]
    )
    fn = joinpath(Rembus.broker_dir(BROKER_NAME), "topic_auth.json")
    open(fn, "w") do io
        write(io, JSON3.write(topic_auth))
    end
end

function run()
    rb = connect()
    # not an admin
    @test_throws RembusError Rembus.enable_debug(rb)
    @test_throws RembusError Rembus.disable_debug(rb)
    @test_throws RembusError Rembus.load_config(rb)
    @test_throws RembusError Rembus.save_config(rb)
    @test_throws RembusError Rembus.broker_config(rb)
    @test_throws RembusError Rembus.private_topics_config(rb)
    @test_throws RembusError Rembus.broker_shutdown(rb)
    close(rb)

    rb = connect(admin)
    try
        Rembus.enable_debug(rb)
        Rembus.disable_debug(rb)
        Rembus.load_config(rb)
        Rembus.save_config(rb)
        cfg = Rembus.broker_config(rb)
        @info "broker config: $cfg"

        cfg = Rembus.private_topics_config(rb)
        @info "private topics config: $cfg"

        Rembus.broker_shutdown(rb)
        @test true
    catch e
        @error "[test_admin_commands] error: $e"
        showerror(stdout, e, catch_backtrace())
        @test false
    finally
        close(rb)
    end
end

admin = "admin"

function setup()
    set_admin(admin)
    set_topic_auth()
end

execute(run, "test_admin_commands", setup=setup)
teardown()
