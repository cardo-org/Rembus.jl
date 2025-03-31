# Configuration

## Broker environment variables

The broker setup is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus|data root directory|
|`REMBUS_KEYSTORE`|\$REMBUS\_ROOT\_DIR/keystore| Directory of broker/server certificate `rembus.crt` and broker/server secret key `rembus.key`|

## Component environment variables

A Rembus component is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus|data root directory|
|`REMBUS_BASE_URL`|ws://localhost:8000|Default base url when defining component with  a simple string instead of a complete url. `@component "myclient"` is equivalent to `@component "ws://localhost:8000/myclient"`|
|`REMBUS_TIMEOUT`|5| Maximum number of seconds waiting for rpc responses|
|`HTTP_CA_BUNDLE`|\$REMBUS\_ROOT\_DIR/ca/rembus-ca.crt|CA certificate|

## Broker configuration files

The broker name determines the directory where the data files are stored.

For example, to set the name of the broker with the companion `broker` script use
the optional `name` argument:

```shell
broker --name my_broker
```

In the following it is assumed the default `broker` name for the broker: in this
case the directory `$REMBUS_DIR/broker` contains the broker states and settings
and secret materials.

```sh
> cd ~/.config/rembus/broker
> tree .

.
├── admins.json
├── bar.acks
├── keys
│   ├── bar.rsa.pem
│   └── foo.ecdsa.pem
├── messages
    ├── 1345
    ├── 345456
    └── 867687
├── router.json
├── tenants.json
├── tenant_component.json
├── topic_auth.json
├── twins
│   ├── bar.json
│   └── foo.json


```

### Secrets and public keys

A file in the `keys` directory, named after the component name, contains the
secret material used to authenticate the component.

This file may contain:

* a RSA public key;
* an ECDSA public key;
* a plaintext shared password string;

To create the RSA or ECDSA  key pairs and send the public key to the broker the
[`register`](@ref "Component registration") method may be employed.

### Components with admin privilege

`admins.json` contains the list of components that have the admin role.
The element of this list are component names.

```text
> cat admins.json
["foo", "bar"]
```

A component with admin privilege may change the privateness level of topics and
authorize other components to bind to private topics.

See [`private_topic`](@ref), [`public_topic`](@ref), [`authorize`](@ref),
[`unauthorize`](@ref) for details.

### Component configuration

The `twins` directory contains a configuration file for each component. The name
of the file is equals to the component name.

the content of the file is a JSON object with the following fields:

* `exposers`: the list of topics exposed by the component.
* `subscribers`: a map where keys are topic names and values are the value of the
   [from](./api.md#subscribe) subscribe option.
* `mark`: a counter value of the more recent received message.

For example, the file `twins/mycomponent.json` is a configuration for the
component `mycomponent`. Such component exposes the RPC service `myservice` and
subscribes to the topic `mytopic` with a `from` value of `3600.0` seconds.

```text
> cat twins/mycomponent.json
{
    "exposers":["myservice"],
    "subscribers":{"mytopic":3600.0},
    "mark":1234
}
```

### Private topics

`topic_auth.json` is a map with `topic` as keywords and an array of component
names as values.
  
For example the following file defines the topic `foo` as private and only the
components `myconsumer` and `myproducer` are allowed to bind to it.

```text
> cat topic_auth.json
{
    "foo":["myconsumer","myproducer"]
}
```

### Multi tenancy

The multi-tenancy feature allows to register components with different
identifiers (names) and to assign them to different tenants. Each tenant
has its own set of components and can manage them independently.

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

* `tenant` is the tenant identifier;

* `pin` is a secret token used for authentication. Value is 8
  digits composed of numbers and the characters `[a-f]`;

* `enabled` consent to disable the tenant and this does not allow to register
  new components. `enabled` is optional and if not present it defaults to `true`.

If the Rembus setup is single tenant then the `tenants.json` file must contain
only the `pin` value to be used by [`register`](@ref "Component registration").

```json
[{"pin": "deadbeef"}]
```

### Components ownership

`tenant_component.json` contains the mapping between the registered component and
the tenant to which it belong.

`tenant` is the tenant identifier and `component` is the component identifier.

For example if the tenant `A` registered the component `foo` then
`tenant_component.json` will be:

```json
[
    {"tenant": "A","component": "foo"}    
]
```

### Published messages database

the files in `messages` store all the published messages received by the broker.
The messages are saved periodically and at broker shutdown.
