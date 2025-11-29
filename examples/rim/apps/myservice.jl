using Rembus

myservice(x, y) = x * y

function start(rb)
    @info "[$rb] starting"
    expose(rb, myservice)
end
