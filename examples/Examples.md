# Examples

To play with the examples gets the required dependencies:

```shell
> alias j='julia --project=. --startup-file=no'
> j -e 'using Pkg; Pkg.instantiate()'
```

and then starts a broker application:

```shell
> j 'using Rembus; caronte()' 
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

@publish announcement("geppeto", "Version 2.0 of pinocchio is a beautiful boy")
@publish announcement("pinocchio", "Cat and Fox are my new friends")
```

To get back all published announcements invoke the RPC service `all_announcements`:

```julia
df = @rpc all_announcements()
```

Oh yes! The returned value is a `DataFrame`.

Below there are some more details just for exposing the core concepts of Rembus.

### The minimal subscriber

In a distributed system governed by the Rembus middleware there are two types of applications, Brokers and Components:

- a Broker routes messages between Components;
- a Component sends and receives messages and may be a publisher, a subscriber, a RPC client, a RPC Server
  or all of these roles;

The first step for a Component application is to bind to a Broker and declare its name:

```julia
@component "ws://caronte.com:8000/organizer"
```

The above declares a component `organizer` that will connect to the broker hosted at `caronte.com` with protocol `ws` served at port `8000`.

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

What remain to do is elevate such julia method as a consumer of the topic `announcement`
using the macro `@subscribe`:

```julia
@subscribe announcement
```

The full code for this minimal `organizer` component that you can run in a REPL is then:

```julia
using Rembus

@component "organizer"

function announcement(username, post)
    println("[$username]: $post")
end

@subscribe announcement

```
