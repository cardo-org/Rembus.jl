# Configuration

## Broker environment variables

The broker setup is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`BROKER_DB`|\$HOME/caronte | Root dir for configuration files and cached messages to be delivered to offline components opting for retroactive mode|
|`BROKER_TCP_PORT`|8000|use `tls://<host>:$BROKER_TCP_PORT` for serving TLS protocol|
|`BROKER_WS_PORT`|8001|use `wss://<host>:$BROKER_WS_PORT` for serving WSS protocol|
|`BROKER_ZMQ_PORT`|8002|ZeroMQ port `zmq://<host>:$BROKER_ZMQ_PORT`|
|`REMBUS_DEBUG`|0| "1": enable debug traces|
|`REMBUS_KEYSTORE`|\$HOME/keystore| Directory of broker certificate `caronte.crt` and broker secret key `caronte.key`|

## Component environment variables

A Rembus component is affected by the following environement variables. 

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_BASE_URL`|ws://localhost:8000|Default base url when defining component with  a simple string instead of a complete url. `@component "myclient"` is equivalent to `@component "ws://localhost:8000/myclient"`|
|`REMBUS_CA`|rembus-ca.crt|CA certificate file name. This file has to be in `$REMBUS_KEYSTORE` directory|
|`REMBUS_DEBUG`|0| "1": enable debug traces|
|`REMBUS_KEYSTORE`|\$HOME/keystore| Directory of CA certificate|
|`REMBUS_TIMEOUT`|5| Maximum number of seconds waiting for rpc responses|
|`HTTP_CA_BUNDLE`|\$REMBUS\_KEYSTORE/\$REMBUS\_CA|CA certificate|

## Database structure

Rembus configuration data and secret materials is persisted to `BROKER_DB` directory.

The database directory has the following layout:

```sh
> cd $REMBUS_DB
> tree .

.
├── admins.json
├── apps
│   ├── bar
│   └── foo
├── impls.json
├── interests.json
├── owners.csv
├── component_owner.csv
├── topic_auth.json
├── twins
    ├── bar
    └── foo


```

where `foo` and `bar` are the component identifiers (`cid`).

In case the component are offline the undelivered messages are temporarly persisted into `twins/bar` and `/twins/foo` files.

### Admin privileges

`admins.json` contains the list of components that have the admin role.
The element of this list is the component `cid`.

```text
> cat admins.json
["foo", "bar"]
```

### RPC implementors

`impls.json` is a map with `topic` as keyword and an array of `cid` as values.

For example:

```text
> cat impls.json
{
    "topic_1":["foo"],
    "topic_2":["foo", "bar"]
}
```

* `foo` component implements `topic_1` and `topic_2` rpc methods.
* `bar` component implements `topic_2` rpc method.

### PubSub subscribers

`twins.json` is a map: the keywords are `cid` and the values are map.
The keys of the map are the subscribed topics and the boolean value is true if the
subscription is retroactive:

```text
> cat twins.json
{
    "mycomponent":{"mytopic1": true, "mytopic2": false}
}
```

`mycomponent` is interested to `mytopic1` and `mytopic2` messages, and `mytopic1` subscribed with option `before_now`:

```julia
@subscribe mytopic1 before_now
```

### Private topics

`topic_auth.json` is a map with `topic` as keywords and an array of `cid` as values.
  
For example if:

```text
> cat topic_auth..json
{
    "foo":["myconsumer","myproducer"]
}
```

then only components `myconsumer` and `myproducer` are allowed to bind to the topic `foo`.

### Users authorized to register components

`owners.csv` is a csv file containing users allowed to register components.

The `pin` column is the PIN token needed for registration.

```text
> cat owners.csv 
pin,uid,name,enabled
482dc7eb,paperoga@topolinia.com,Paperoga,false
58e26283,paperino@topolinia.com,Paperino,false
```

### Components ownership

`component_owner.csv` is a csv file containing the mapping between the registered components and the user that performed the registration.

`uid` is the user identity and `component` is the component identifier.

For example if the user Paperoga registered the component `foo` then:

```text
> cat component_owner.csv
uid,component
paperoga@topolinia.com,foo
```
