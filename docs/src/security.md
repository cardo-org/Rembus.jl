# Security

End-to-end encryption is provided by Secure Web Socket (`wss`) and Transport Layer Security
(`tls`) protocols.

Authentication and authorization are realized by using RSA keys. The component owns a secret
key and the broker knows its public key.

## End-to-end encryption

The broker requires that in the directoy `$REMBUS_KEYSTORE` there are:

* The server certificate `rembus.crt`;
* The private key `rembus.key`;

The component requires a CA bundle or the CA certificate of the authority that signed the
broker certificate. The environment variable `HTTP_CA_BUNDLE` may be used to specify the
the CA file.

## Authentication

For enabling authentication a previsous exchange of a public key or a shared secret must be performed.

If the secret material is not know to the broker then the connection phase skips the authentication
steps and the named component connects but without any privilege reserved to authenticated
and authorized components.  

For a component with name `foobar` that connect to the broker the authentication mechanism involves
the following steps.

1. The component sends a message declaring the name `foobar`;
1. If exists the file `$BROKER_DB/keys/foobar` then the broker replies with a random challenge;
1. The component calculates a digest, if the the `$HOME/.config/rembus/keys/foobar` file is a RSA private key:
    * Make a SHA256 digest of a string containing the challenge plus the name `foobar` and signs the digest with the private key.
    * Otherwise consider the file content as a shared secret and make a SHA256 digest of the string containing the challenge plus the shared secret.
1. The component send the digest to the broker;
1. The broker verifies the digest, if the the `$HOME/.config/broker/keys/foobar` file is a RSA public key:
    * Verify that the received digest of string containing the challenge plus the name `foobar` is signed by the corresponding private key;  
    * Otherwise verify that the received digest equals to the digest of the string containing the
    challenge plus the shared secret;
1. Return SUCCESS if authentication succeed, otherwise return ERROR and close the connection.

## Authorization

By default topics are considered public, that is accessible by all components.

If the topic is declared private then only authorized components may access it
with the methods:

* publish
* rpc
* expose/unexpose
* subscribe/unsubscribe

The topic visibility may be changed by a component with `admin` role:

```julia
rb = connect("superuser")

# public -> private
private_topic(rb, "my_topic")

# private -> public
public_topic(rb, "my_topic")
```

To execute such actions the `admin` role must be assigned to the component `superuser`:
its name must be present in the file `$HOME/.config/broker/admins.json`

More then one component may be assigned the `admin` role:

```json
# admis.json
["superuser", "foobar"]
```

## Component registration

Authenticated components can be provisioned with the [`Rembus.register`](@ref)
method.

```julia
register(component_name, pin; scheme=SIG_RSA)
```

* `component_name`: This is the name of the component. The domain part of the name defines
   its tenant. For example, `foo.xyz` belongs to the `xyz` tenant, while `foo` (wothout a
   domain) belongs to the default tenant (`.`).
* `pin`: This is the tenant's secret PIN code.
* `scheme`: This optional parameter specifies the signature scheme to use and can be:
  * `SIG_RSA`: for RSA Encryption.
  * `SIG_ECDSA`: for Elliptic Curve Digital Signature Algorithm.
