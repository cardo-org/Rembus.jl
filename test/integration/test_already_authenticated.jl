include("../utils.jl")

function run()
    rb = connect("myc")

    @component "myc"
    @publish foo()

    sleep(1)
    close(rb)
    @shutdown
end

execute(run, "test_already_authenticated")
