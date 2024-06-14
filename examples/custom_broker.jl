using Rembus

module CarontePlugin

using Rembus

export myservice

function myservice(component, x, y)
    @info "request from [$component], authenticated:$(isauthenticated(component))"
    return x + y
end

expose(ctx, router, component, msg) = @info "[$component] exposing $(msg.topic)"
unexpose(ctx, router, component, msg) = @info "[$component] unexposing $(msg.topic)"
subscribe(ctx, router, component, msg) = @info "[$component] subscribing $(msg.topic)"
unsubscribe(ctx, router, component, msg) = @info "[$component] unsubscribing $(msg.topic)"

end

caronte(plugin=CarontePlugin)
