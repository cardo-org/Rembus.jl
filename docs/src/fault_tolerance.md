# Fault-tolerance features

Beside struggling to provide a simple and lean API one of the main points of Rembus is
its ability to be fault-tolerant respect to networks and application failures.

The [Macro-based API](@ref) and the [`component`](./api.md#component) method provide
an automatic reconnection policy in case of network faults and try at the best to
guarantee message delivery when faults happen.

This mean that the following RPC service will run forever and it will reconnect
automatically in case of network failures or broker unavailability.

```julia
using Rembus

@component "mycomponent"

function myservice(input::DataFrame)
    # run your super-cool logic and get back the result
    output_df = my_logic(input)
    return output_df
end

@expose myservice

@wait
```

Fault-tolerance holds equally for publish/subscribe setups: in case of connection lost the subscriber retries to reconnect to the broker until the connection will be up again.   

If the subscription use the `before_now` option then messages published whereas the
component was offline are delivered ordered by time of publishing when the component
get online again.

```julia
using Rembus

@component "consumer"

function mytopic(input::DataFrame)
    # consume the dataframe posted to mytopic topic
end

@subscribe mytopic before_now

@wait
```
