include("../utils.jl")

function run()
    rb = connect("myc")

    @component "myc"
    @publish foo()

    sleep(1)
    close(rb)
    @terminate
end

execute(run, "test_already_authenticated")
