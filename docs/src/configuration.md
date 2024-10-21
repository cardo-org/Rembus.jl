# Configuration

## Broker environment variables

The broker setup is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus|data root directory|
|`BROKER_WS_PORT`|8000|default port for serving WebSocket protocol|
|`REMBUS_DEBUG`|| "1": enable debug traces|
|`REMBUS_KEYSTORE`|\$REMBUS\_ROOT\_DIR/keystore| Directory of broker/server certificate `rembus.crt` and broker/server secret key `rembus.key`|

## Component environment variables

A Rembus component is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus|data root directory|
|`REMBUS_BASE_URL`|ws://localhost:8000|Default base url when defining component with  a simple string instead of a complete url. `@component "myclient"` is equivalent to `@component "ws://localhost:8000/myclient"`|
|`REMBUS_DEBUG`|| "1": enable debug traces|
|`REMBUS_TIMEOUT`|5| Maximum number of seconds waiting for rpc responses|
|`HTTP_CA_BUNDLE`|\$REMBUS\_ROOT\_DIR/ca/rembus-ca.crt|CA certificate|

## Broker configuration files

The broker name determines the directory where the data files are stored.

For example, to set the name of the broker with the companion `broker` script use
the optional `name` argument:

```shell
broker --name my_broker
```

In the following it is assumed the default `caronte` name for the broker: in this
case the directory `$REMBUS_DIR/caronte` contains the broker settings and secret materials.

```sh
> cd ~/.config/rembus/caronte
> tree .

.
├── admins.json
├── keys
│   ├── bar.rsa.pem
│   └── foo.ecdsa.pem
├── exposers.json
├── tenants.json
├── component_owner.json
├── topic_auth.json
├── twins.json
├── messages
    ├── 1345
    ├── 345456
    └── 867687

```

### Secret material for authenticated components

A file in the `keys` directory contains the secret material used to authenticate the component.

This file may contain:

* a RSA public key;
* an ECDSA public key;
* a plaintext shared password string;

To create the RSA or ECDSA  key pairs and send the public key to the broker the [`Rembus.register`](@ref) method may be employed.

### Components with admin privilege

`admins.json` contains the list of components that have the admin role.
The element of this list are component names.

```text
> cat admins.json
["foo", "bar"]
```
A component with admin privilege may change the privateness level of topics and authorize other components to bind to private topics.

See [`private_topic`](@ref), [`public_topic`](@ref), [`authorize`](@ref), [`unauthorize`](@ref) for details.

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
The keys of the map are subscribed topics and values are floats that
define time periods in microseconds. A message received before the node subscription time
but included in the time period will be delivered to the node.

For example, in the following setting `mytopic1` does not get messages received before the subscription
because the time period is zero, while messages more recent than one hour are delivered to `topic2`:

If the time period is `Inf` then all messages received before the time of subscription and not delivered
because the node was offline are sent to it.

```text
> cat subscribers.json
{
    "mycomponent":{"mytopic1": 0.0, "mytopic2": 3.6e9}
}
```

For declaring interest for all messaged delivered wheh the node was offline with [Macro-based API](@ref)
use `from=LastReceived()` expression:

```julia
@subscribe mytopic1 from=LastReceived()
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

Authenticated components may be provisioned with the [`Rembus.register`](@ref) method.

```julia
register(component_name, uid, pin, key_type=SIG_RSA)
```

`key_type` may be equal to `SIG_RSA` for RSA Encryption and equal to `SIG_ECDSA` for Elliptic Curve Digital Signature Algorithm.

`register` requires a tenant and a pin that must match with one of the users defined in `tenants.json` file. 

`tenants.json` file example:

```json
[
    {
        "tenant": "A",
        "pin": "482dc7eb",
        "enabled": true
    },
    {
        "tenant": "B",
        "pin": "58e26283",
    },

]
```

`tenant` is the tenant identifier.

`pin` is a secret token used for authentication. The `pin` column is a 8 digits string 
composed of numbers and the characters `[a-f]`. 

`enabled` consent to disable the tenant and this does not allow to register new components.

`enabled` is optional and if not present it defaults to `true`.

### Components ownership

`tenant_component.json` contains the mapping between the registered component and
the tenant to which it belong.

`tenant` is the tenant identifier and `component` is the component identifier.

For example if the tenant `A` registered the component `foo` then `tenant_component.json` 
will be:

```json
[
    {"tenant": "A","component": "foo"}    
]
```

### Files reserved to the broker

`twins.json` get saved at broker shutdown and contains, for each component,
the reference for the last message delivered to the component.
It is a file managed by the broker, do no edit this file.

the files in `messages` directory are parquet files that get saved periodically and at broker shutdown and contain all the published messages.
