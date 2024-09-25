# Configuration

## Broker environment variables

The broker setup is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_ROOT_DIR`|\$HOME/.config/rembus|data root directory|
|`BROKER_WS_PORT`|8000|default port for serving WebSocket protocol|
|`REMBUS_DEBUG`|| "1": enable debug traces|
|`REMBUS_KEYSTORE`|\$REMBUS\_ROOT\_DIR/keystore| Directory of broker/server certificate `rembus.crt` and broker/server secret key `rembus.key`|

## Component environment variables

A Rembus component is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_ROOT_DIR`|\$HOME/.config/rembus|data root directory|
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
case the directory `$REMBUS_ROOT_DIR/caronte` contains the broker settings and secret materials.

```sh
> cd ~/.config/rembus/caronte
> tree .

.
├── admins.json
├── keys
│   ├── bar.rsa.pem
│   └── foo.ecdsa.pem
├── exposers.json
├── owners.json
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

For declaring retroactiveness with [Macro-based API](@ref) use the option `before_now`:

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

Authenticated components may be provisioned with the [`Rembus.register`](@ref) method.

```julia
register(component_name, uid, pin, key_type=SIG_RSA)
```

`key_type` may be equal to `SIG_RSA` for RSA Encryption and equal to `SIG_ECDSA` for Elliptic Curve Digital Signature Algorithm.

`register` requires a username and a pin that must match with one of the users defined in `owners.json` file. 

`owners.json` file example:

```json
[
    {
        "uid": "paperoga@topolinia.com",
        "name": "Fethry Duck",
        "pin": "482dc7eb",
        "enabled": true
    },
    {
        "uid": "paperino@topolinia.com",
        "name": "Donald Fauntleroy Duck",
        "pin": "58e26283",
        "enabled": true
    },

]
```

`uid` is the username.

`pin` is a secret token used for authentication. The `pin` column is a 8 digits string 
composed of numbers and the characters `[a-f]`. 

`name` is an optional string describing the user.

`enabled` consent to disable the user for
registering components. `enabled` is optional and if not present it defaults to `true`.

### Components ownership

`component_owner.json` contains the mapping between the registered components and
the user that performed the registration with `register` API.

`uid` is the user identity and `component` is the component identifier.

For example if the user Paperoga registered the component `foo` then `component_owner.json` 
will be:

```json
[
    {"uid": "paperoga@topolinia.com","component": "foo"}    
]
```

### Files reserved to the broker

`twins.json` get saved at broker shutdown and contains, for each component,
the reference for the last message delivered to the component.
It is a file managed by the broker, do no edit this file. 

the files in `messages` directory are parquet files that get saved periodically and at broker shutdown and contain all the published messages.
