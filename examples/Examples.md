# Examples

To play with the examples gets the required dependencies:

```shell
> alias j='julia --project=. --startup-file=no'
> cd examples
> j -e 'using Pkg; Pkg.instantiate()'
```

and then starts a broker application:

```shell
> j -e 'using Rembus; broker() |> wait' 
```

## greeter.jl

This is the "hello world" example for Rembus.

A server component that implements the method `say_hello` that expects
a name and reply with a greeting:

```julia
using Rembus

@expose say_hello(name) = "hello $name"
@wait
```

A client component that invoke `say_hello`

```julia
using Rembus

println(@rpc say_hello("Julia"))
```

## subscriber.jl

In standard parlance `subscriber.jl` example implements a message subscriber/consumer: an application that listen to messages published to a set of logical channels, usually called topics.

Indeed the example implements also a RPC service and show that a DataFrame-based message is a first-class citizen of the Rembus middleware.

To see it in action:

```shell
> j subscriber.jl
```

Open a REPL and send some messages:

```julia
using Rembus

@publish announcement("geppeto", "Pinocchio 2.0 is a real boy")
@publish announcement("pinocchio", "Cat and Fox are my new friends")
```

To get back all published announcements invoke the RPC service `all_announcements`:

```julia
df = @rpc all_announcements()
```

The returned value is a `DataFrame`.

Below there are some more details just for exposing the core concepts of Rembus.

### The minimal subscriber

In a distributed system governed by the Rembus middleware there are two types of applications, Brokers and Components:

- a Broker routes messages between Components;
- a Component sends and receives messages and may be a publisher, a subscriber, a RPC client, a RPC Server
  or all of these roles;

The first step for a Component application is to bind to a Broker and declare its name:

```julia
@component "ws://broker.com:8000/organizer"
```

The above declares a component `organizer` that will connect to the broker hosted at `broker.com` with protocol `ws` served at port `8000`.

> There are some sensible defaults that may help to keep the code clean:
> the url of the broker may be set with the environment variable REMBUS_BASE_URL:
>
> `export REMBUS_BASE_URL=<protocol>://<host>:<port>`
>
> If `REMBUS_BASE_URL` is not defined the default url will be `ws://127.0.0.1:8000`.
>
> In this case the component may be declared as:
>
> `@component "organizer"`

Suppose the `organizer` wants to receive all messages published to the `announcement` topic.

The messages are expected to have two fields: an username and a string containing a message announcement.

> In general messages exchanged between distributed components may have any numbers of fields with primitive types mapped to [CBOR](https://www.rfc-editor.org/rfc/rfc8949.html#name-cbor-data-models) types.

How do we consume such messages?

With a method which name equals to the topic name and with a number of arguments equals to the message fields:

```julia
function announcement(username, post)
    # do something with the post of username
end
```

What remain to do is declare the method as a consumer of the topic `announcement`
using the macro `@subscribe` folowed by `@reactive` to enable the sending of
pub/sub messages to the component and `@wait` for the main loop:

```julia
@subscribe announcement
@reactive
@wait
```

The full code for this minimal `organizer` component that you can run in a REPL is then:

```julia
using Rembus

@component "organizer"

function announcement(username, post)
    println("[$username]: $post")
end

@subscribe announcement
@reactive
@wait
```

## hierarchy_broker.jl

This example shows how to extends the broker with custom logic.

The broker plugin implements pub/sub message routing using the
a topic expression language on the same lines of [zenoh](https://zenoh.io/docs/manual/abstractions/) design of the key/value spaces.

Start the broker:

```shell
> j hierarchy_broker.jl
```

Start a component that uses a topic expression to subscribe to a space of topics:

```julia
using Rembus

consume(topic, value) = @info "$topic = $value"

rb = connect()

subscribe(rb, "a/*/c", consume)
reactive(rb)
wait(rb)
```

Please note that the first argument of the subscribed function `consume` is
the topic used by the publisher. This is because you are subscribed to a space of topics and the information about the specific topic will otherwise be lost.

Then publish on different topics to see this hierarchical topic subscrition
in action:

```julia
using Rembus

rb = connect()
publish(rb, "a/foo/c", "RECEIVED")
publish(rb, "a/bar/c", "RECEIVED")
publish(rb, "a/b/x/c", "NOT RECEIVED")
publish(rb, "b/b/c", "NOT RECEIVED")

# The sealed topic
publish(rb, "a/@v1/c", "NOT RECEIVED")

```

## rocket.jl

This simple example show how to integrate Rembus with
[Rocket](https://github.com/ReactiveBayes/Rocket.jl), a reactive extensions library for Julia.

The integration is easily obtained using the `@inject` macro: the first argument of method to be
subscribed to `my_topic` is a `Subject` object.

The method body simply consists in a `next!()` call: feeding the value received from the topic
to the Subject make it observabled to all actors subscribed to the Subject.

For example a function actor may be used to publish a message to topic `alarm`
if the value received from `my_topic` is greater then 100:

```julia
function my_topic(actor, rb, n)
    next!(actor, n)
end

subject = Subject(Number)

keeper = keep(Number)

# send an alarm if the number published on value is greater than 100
subscribe!(
    subject |> filter((n) -> n > 100),
    (n) -> @publish alarm("critical value: $n")
)

subscribe!(subject, keeper)
subscribe!(subject, logger())

@component "rocket"
@inject subject
@subscribe my_topic from=Rembus.LastReceived
@reactive
```

To see in action, start the subscriber in a REPL:

```shell
terminal_1> j -i rocket.jl
```

the REPL may be useful inspecting all the messaging received using the subscribed
`KeepActor`:

```julia
getvalues(keeper)
```

Start another process to listen for alarms:

```julia
using Rembus

@subscribe alarm(msg) = println(msg)
@reactive
@wait
```

In another REPL publish messages:

```julia
using Rembus

# no alarm is produced
@publish my_topic(50)

# above threshold, an alarm message is published
@publish my_topic(150)
```

## server.jl

Rembus may be used in a simpler client-server architecture, without a decoupling broker.

A server accepting connections may be setup with these lines:

```julia
    rb = server()
    inject(rb, Ctx(2))
    
    # declare exposed methods
    expose(rb, power)
    expose(rb, set_exponent)
    
    wait(rb)
```

The exposed method have the following signature:

```julia
function power(ctx, component, df::DataFrame)
    df.y = df.x .^ ctx.exponent
    return df
end
```

Where:

- `ctx` is a state value passed to the `server` constructor and shared
between all provided methods.
- `component` is an object representing the component that made the request.

The `component` value may be useful when the method has to be available only to
authenticated components:

```julia
function power(ctx, component, df::DataFrame)
    isauthenticated(component)  || error("unauthorized")
    df.y = df.x .^ ctx.exponent
    return df
end
```

