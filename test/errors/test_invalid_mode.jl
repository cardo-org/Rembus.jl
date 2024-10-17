include("../utils.jl")

@test_throws ErrorException caronte(mode="invalid_mode")
