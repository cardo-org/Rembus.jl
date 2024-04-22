# Configuration

## Broker environment variables

The broker setup is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`BROKER_DIR`|\$HOME/.config/caronte | Root dir for configuration files and cached messages to be delivered to offline components opting for retroactive mode|
|`BROKER_TCP_PORT`|8000|use `tls://<host>:$BROKER_TCP_PORT` for serving TLS protocol|
|`BROKER_WS_PORT`|8001|use `wss://<host>:$BROKER_WS_PORT` for serving WSS protocol|
|`BROKER_ZMQ_PORT`|8002|ZeroMQ port `zmq://<host>:$BROKER_ZMQ_PORT`|
|`REMBUS_DEBUG`|0| "1": enable debug traces|
|`REMBUS_KEYSTORE`|\$BROKER\_DIR/keystore| Directory of broker certificate `caronte.crt` and broker secret key `caronte.key`|

## Component environment variables

A Rembus component is affected by the following environement variables. 

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus| Root dir for component configuration files|
|`REMBUS_BASE_URL`|ws://localhost:8000|Default base url when defining component with  a simple string instead of a complete url. `@component "myclient"` is equivalent to `@component "ws://localhost:8000/myclient"`|
|##`REMBUS_CA`|rembus-ca.crt|CA certificate file name. This file has to be in `$REMBUS_KEYSTORE` directory|
|`REMBUS_DEBUG`|0| "1": enable debug traces|
|##`REMBUS_KEYSTORE`|\$REMBUS\_DIR/keystore| Directory of CA certificate|
|`REMBUS_TIMEOUT`|5| Maximum number of seconds waiting for rpc responses|
|`HTTP_CA_BUNDLE`|\$REMBUS\_DIR/ca/rembus-ca.crt|CA certificate|
|##`HTTP_CA_BUNDLE_OLD`|\$REMBUS\_KEYSTORE/\$REMBUS\_CA|CA certificate|

## Broker configuration files

The directory `BROKER_DIR` contains broker settings and secret materials.

```sh
> cd $BROKER_DIR
> tree .

.
├── admins.json
├── keys
│   ├── bar
│   └── foo
├── exposers.json
├── owners.csv
├── component_owner.csv
├── topic_auth.json
├── twins
    ├── bar
    └── foo


```

where `foo` and `bar` are example component names.

In case the component are offline the undelivered messages are temporarly persisted into `twins/bar` and `/twins/foo` files.

### Admin privileges

`admins.json` contains the list of components that have the admin role.
The element of this list are component names.

```text
> cat admins.json
["foo", "bar"]
```

### RPC exposers

`exposers.json` is a map with `topic` as keyword and an array of component names as values.

The `topic` keyword is the name of an RPC method exposed by all components present in the
value array.

For example:

```text
> cat exposers.json
{
    "topic_1":["foo"],
    "topic_2":["foo", "bar"]
}
```

* `foo` component exposes `topic_1` and `topic_2` rpc methods.
* `bar` component exposes `topic_2` rpc method.

### Pub/Sub subscribers

The topics subscribers are persisted in the file `subscribers.json`. Only named component
are persisted.

`subscribers.json` contains a map: keywords are component names and values are maps.
The keys of the map are subscribed topics and values are booleans that
assert if the topic is retroactive:

```text
> cat subscribers.json
{
    "mycomponent":{"mytopic1": true, "mytopic2": false}
}
```

`mycomponent` is subscribed to `mytopic1` and `mytopic2` topics, and `mytopic1` is retroactive,
namely when it connects it wants to receive messages published when it was offline.

For declaring retroactiveness with [Supervised API](@ref) use the option `before_now`:

```julia
@subscribe mytopic1 before_now
```

### Private topics

`topic_auth.json` is a map with `topic` as keywords and an array of component names as values.
  
For example:

```text
> cat topic_auth.json
{
    "foo":["myconsumer","myproducer"]
}
```

asserts then only components `myconsumer` and `myproducer` are allowed to bind to the topic `foo`.

### Users allowed to register components

Authenticated components may be provisioned with the `register` API method.

```julia
register(component_name, uid, pin)
```

`register` requires a username and a pin that must match with one of the entries of `owners.csv` file.

```text
> cat owners.csv 
pin,uid,name,enabled
482dc7eb,paperoga@topolinia.com,Fethry Duck,false
58e26283,paperino@topolinia.com,Donald Fauntleroy Duck,false
```

The `pin` column is the PIN token needed for registration, `uid` column is the username,
`name` is an optional string describing the user and `enabled` consent to stop the user for
registering components.

### Components ownership

`component_owner.csv` is a csv file containing the mapping between the registered components and
the user that performed the registration with `register` API.

`uid` is the user identity and `component` is the component identifier.

For example if the user Paperoga registered the component `foo` then:

```text
> cat component_owner.csv
uid,component
paperoga@topolinia.com,foo
```
