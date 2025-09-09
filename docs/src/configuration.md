# Configuration

## Environment variables

A component is affected by the following environment variables.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_DIR`|\$HOME/.config/rembus|Rembus root directory|
|`REMBUS_BASE_URL`|ws://localhost:8000|Default base url when defining component with  a simple string instead of a complete url. `@component "myclient"` is equivalent to `@component "ws://localhost:8000/myclient"`|
|`REMBUS_TIMEOUT`|5| Maximum number of seconds waiting for rpc responses|
|`REMBUS_ACK_TIMEOUT`|5| Maximum number of seconds waiting for pub/sub ack messages|
|`REMBUS_CHALLENGE_TIMEOUT`|5|  Time interval after which the connection is closed if a challenge response is not received when `connection_mode` is `authenticated`
|`REMBUS_RECONNECT_PERIOD`|1| Reconnection retry period|
|`REMBUS_CACHE_SIZE`|1000000| Max numbers of pub/sub messages cached in memory|
|`HTTP_CA_BUNDLE`|\$REMBUS\_DIR/ca/rembus-ca.crt|CA certificate|

## Broker only environment variables

When a component is a broker the setup is affected also by the `REMBUS_KEYSTORE`
variable that define the directory where are stored the private key and the server
certificate needed for wss and tls secure connections.

| Variable |Default| Descr |
|----------|-------|-------|
|`REMBUS_KEYSTORE`|\$REMBUS\_DIR/keystore| Directory of broker/server certificate `rembus.crt` and broker/server secret key `rembus.key`|

## Broker configuration files

The broker name determines the directory where settings and component related data are
stored.

For example, to set the name of the broker with the companion `broker` script use
the optional `name` argument:

```shell
broker --name my_broker
```

In the following it is assumed the default `broker` name for the broker: in this
case the directory `$REMBUS_DIR/broker` contains the broker configuration data
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
├── settings.json
├── tenants.json
├── topic_auth.json
├── twins
│   ├── bar.json
│   └── foo.json


```

### Component settings

The behavoir of a component may be configured with the properties defined in the
`settings.json` file. The values from `settings.json` take priority over environment
variables values.

The following properties are supported (in parenthesis the corresponding environment
variable):

- `cache_size (REMBUS_CACHE_SIZE)`: max numbers of pub/sub messages cached in memory;
- `connection_mode`: `"authenticated"` or `"anonymous"` for components without name or
   authentication credentials;
- `ack_timeout (REMBUS_ACK_TIMEOUT)`: timeout in seconds for pub/sub ack messages;
- `challenge_timeout (REMBUS_CHALLENGE_TIMEOUT)`: time interval after which the connection
   is closed if a challenge response is not received. This feature holds only if
   `connection_mode` is `authenticated`;
- `request_timeout (REMBUS_TIMEOUT)`: maximum time in seconds for waiting a rpc response;
- `overwrite_connection`: If `true` a connecting component with the same name of an
   already connected component connect successfully and the already connected component is
   disconnected from the broker.
- `reconnect_period (REMBUS_RECONNECT_PERIOD)`: reconnection retry period when connection is
   down;
- `stacktrace`: When an exception occurs the error stack trace is logged if `stacktrace` is
   `true`.
- `zmq_ping_interval (REMBUS_ZMQ_PING_INTERVAL)`: ZMQ ping interval in seconds;
- `ws_ping_interval (REMBUS_WS_PING_INTERVAL)`: WebSocket ping interval in seconds;

```json
{
    "cache_size": 1000000,
    "connection_mode": "anonymous",
    "ack_timeout": 2,
    "challenge_timeout": 3,
    "request_timeout": 5,
    "overwrite_connection": false,
    "reconnect_period": 1,
    "stacktrace": false,
    "zmq_ping_interval": 30,
    "ws_ping_interval": 30
}
```

### Secrets and public keys

A file in the `keys` directory, named after the component name, contains the
secret material used to authenticate the component.

This file may contain:

* a RSA public key;
* an ECDSA public key;
* a plaintext shared password string;

To create the RSA or ECDSA  key pairs and send the public key to the broker the
[`register`](@ref "Component Registration") method may be employed.

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

The `twins` directory contains a configuration file for each component. The name of the file
corresponds to the component name.

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

### Multi-Tenancy

The multi-tenancy feature enables the logical isolation of components by assigning them to
specific tenants.

Communication is restricted: only components belonging to the same tenant
can interact with each other.

A component's tenant is determined by the domain part of its name. For instance, the
component `foo.org` belongs to the `org` tenant. Components without a domain
(e.g., `my_component`) are automatically assigned to the default tenant, represented by
a single dot (`.`).

To register components, each tenant must possess a unique secret PIN. This PIN is an
8-character hexadecimal string, configured within the `tenants.json` file.

`tenants.json` File Example (Multi-Tenant Configuration):

```json
{
  "org": "deadbeef",
  "com": "482dc7eb"
}
```

For single-tenant Rembus setups, the `tenants.json` file should contain only one PIN value,
designated for the default tenant. This PIN will be used for all component registrations.

```json
{
  ".": "deadbeef"
}
```

### Published messages database

the files in `messages` store all the published messages received by the broker.
The messages are saved periodically and at broker shutdown.
