using Rembus

module CarontePlugin

using Rembus

export myservice

function myservice(ctx, component, x, y)
    @info "request from [$component], authenticated:$(isauthenticated(component))"
    return x + y
end

expose_handler(ctx, router, component, msg) = @info "[$component] exposing $(msg.topic)"
unexpose_handler(ctx, router, cmp, msg) = @info "[$cmp] unexposing $(msg.topic)"
subscribe_handler(ctx, router, cmp, msg) = @info "[$cmp] subscribing $(msg.topic)"
unsubscribe_handler(ctx, router, cmp, msg) = @info "[$cmp] unsubscribing $(msg.topic)"

end

broker(plugin=CarontePlugin)
