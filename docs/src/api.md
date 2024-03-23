# Plain API

There is a set of functions that provide a plain API:

- connect
- publish
- rpc
- expose
- subscribe
- unexpose
- unsubscribe
- close

The `connect` function returns a connection handle used by the other APIs for exchanging data and commands.

> This API does not provide automatic reconnection in case of network
failures, if this happen the exception must be handled explicitly by the application.

```julia
using Rembus

rb = connect("mycomponent")

publish(rb, "metric", Dict("name"=>"trento/castello", "var"=>"T", "value"=>21.0))

close(rb)
```
