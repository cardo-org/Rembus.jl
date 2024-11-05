include("../utils.jl")

@test_throws ErrorException broker(mode="invalid_mode")
