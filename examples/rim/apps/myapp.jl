using Rembus

add(x, y) = x + y

function start(rb)
    @info "[$rb] starting"
    expose(rb, add)
end
