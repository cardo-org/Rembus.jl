# Supervised API

Rembus aims to write distributed applications simple and fun.

But beside struggling to provide a simple and lean API one of the main points of Rembus is its ability to be fault-tolerant respect to networks and application failures.

For example the following RPC service will run forever and it will reconnect
automatically to the broker in case of network failures or to broker unavailabity due
to shutdown or failures, there aren't boilerplates for reliable connection management.

```julia
@component "mycomponent"

function myservice(input::DataFrame)
    # run your super-cool logic and get back the result
    result = my_logic(input)
    return df
end

@expose myservice

forever()
```

Fault-tolerance holds equally for publish/subscribe setups: connection failures recovers automatically and published messages are cached and delivered as soon as possible.
