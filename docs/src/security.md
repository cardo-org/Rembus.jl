# Security

End-to-end encryption is provided by **Secure Web Socket** (`wss`) and
**Transport Layer Security** (`tls`).

Authentication and authorization are realized by using RSA and ECDSA keys
(digital signature schemes).

Each client component owns a private key, while the server/broker holds the
corresponding public key.

In this document, the syntax \$VARNAME indicates the environment variable
VARNAME.

## End-to-end Encryption

The broker requires the following files in the directory `\$REMBUS_KEYSTORE`:

* The server certificate: `rembus.crt`
* The server private key: `rembus.key`

The client component must have access to a CA bundle or the CA certificate
of the authority that signed the broker’s certificate.

The environment variable `HTTP_CA_BUNDLE` may be used to specify this CA file.

## Authentication

To enable authentication, a previous exchange of a public key or a shared secret
must be performed.

If the broker does not recognize the client’s key or secret, the connection will
still be established, but the component will be treated as unauthenticated and
will not have access to privileges reserved for authenticated components.

For a component named `foobar`, the authentication steps are:

1. The component sends a message declaring its name `foobar`.
1. If the file `\$BROKER_DB/keys/foobar` exists, the broker replies with a
   random challenge.
1. The component computes a response:

    * If `\$HOME/.config/rembus/keys/foobar` contains an RSA or ECDSA private key:
        * Create a SHA-256 digest of the string `(challenge || "foobar")`.
        * Sign this digest with the private key.

    * Otherwise, treat the file content as a shared secret and compute the
      SHA-256 digest of `(challenge || shared_secret)`.
1. The component send the resulting digest (or signature) to the broker;
1. The broker verifies the response:
    * if `\$HOME/.config/broker/keys/foobar` contains an RSA or ECDSA public key:
        * Verify that the signature matches `(challenge || "foobar")`.  
    * Otherwise verify that the digest equals the SHA-256 of
      `(challenge || shared_secret)`.
1. If verification succeeds, the broker returns **SUCCESS**; otherwise,
   it returns **ERROR** and closes the connection.

## Authorization

By default, all topics are **public**, meaning accessible by any component.

If a topic is declared **private**, only authorized components may use the following methods:

* `publish`
* `rpc`
* `expose/unexpose`
* `subscribe/unsubscribe`

Topic visibility can be changed by a component with the `admin` role:

```julia
rb = connect("superuser")

# public -> private
private_topic(rb, "my_topic")

# private -> public
public_topic(rb, "my_topic")
```

To perform these actions, the `admin` role must be assigned to the component
`superuser`, whose name must appear in the file
`\$HOME/.config/broker/admins.json`.

Multiple components can be granted the `admin` role:

```json
# admis.json
["superuser", "foobar"]
```

## Component Registration

Authenticated components can be provisioned with the [`Rembus.register`](@ref)
method.

This method generates an RSA or ECDSA key pair and distributes the public key to
the broker.

```julia
register(component_name, pin; scheme=SIG_RSA)
```

* `component_name`: The name of the component. The domain part defines its
   tenant.
  * Example: `foo.xyz` belongs to the tenant `xyz`, while `foo` (without a
      domain) belongs to the default tenant (`.`).
* `pin`: The tenant's secret PIN code.
* `scheme`: The signature scheme to use (optional):
   can be:
  * `SIG_RSA` - RSA digital signatures
  * `SIG_ECDSA` - Elliptic Curve Digital Signature Algorithm
