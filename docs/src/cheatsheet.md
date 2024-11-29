# Rembus Cheat Sheet

## Startup and teardown

Connect to the broker with identity `myname`:

```julia
@component "myname"
```

Close the connection and shutdown the component:

```julia
@shutdown
```

Loop unless `Ctrl-C` or `shutdown()`:

```julia
forever()
```

> **NOTE:** `forever` is required by `@subscribe` and `@expose` unless you are in the REPL.

Terminate background Rembus task and return from `forever()`:

```julia
shutdown()
```

## Pub/Sub: 1 publisher and N subscribers  

Publish a message with topic `mytopic` and data payload that is the CBOR encoding
of `[arg1, arg2, arg3]`:

```julia
@publish mytopic(arg1, arg2, arg3)
```

Subscribe to topic `mytopic`, the arguments `arg1, arg2, arg3` are the CBOR decoded
values of the data payload:

```julia
# Method `mytopic` is called for each published message.
function mytopic(arg1, arg2, arg3)
    # do something
end

# Two different modes of subscription:
@subscribe mytopic from_now # declare interest to topic mytopic handling newer messages 
@subscribe mytopic before_now # messages from the past and not received because offline
@subscribe mytopic # default to from_now  
```

Start and stop to call subscribed methods when a published message is received:

```reactive
@reactive
@reactive_off
```

Remove the topic subscription:

```julia
@unsubscribe mytopic
```

By default reactive in enabled.

## Remote Procedure Call

Call the remote method `myrpc` exposed by a component:

```julia
response = @rpc myrpc(arg1, arg2)
```

> **NOTE:** in case of successfull invocation the `response` value is the remote method return value, othervise an exception is thrown.

Expose a method implementation:

```julia
function myrpc(arg1, arg2)
    # evaluate body and return response ...
    return response
end

@expose myrpc(arg1, arg2)
```

Stop to serve the RPC method:

```julia
@unexpose myrpc
```


