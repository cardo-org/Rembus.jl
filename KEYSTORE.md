# Secure connections setup

Encrypted WebSocket and TLS connections need keys and certificates adhering to the following rules:

- The broker/server private key is named `rembus.key`.
- The broker/server signed certificate is named `rembus.crt`.
- The environment variable `REMBUS_KEYSTORE` define the directory where `rembus.key` and `rembus.crt` must be located.
- The full path of the certificate or the bundle containing the CA that signed `rembus.crt` must be specified with the standard env variable `HTTP_CA_BUNDLE`.

if env variable `REMBUS_KEYSTORE` is not defined the default for the keystore directory is
`$HOME/.config/rembus/keystore`.

The keystore directory path may be retrieved with: 

```julia
using Rembus

Rembus.keystore_dir()
```

`$REMBUS_KEYSTORE/ca/rembus-ca.crt` is the default value for `HTTP_CA_BUNDLE` if it is unset.

The CA certificate may be retrieved with:

```julia
using Rembus

Rembus.rembus_ca()
```


## Secrets materials: quick bootstrapping  

The utility script `init_keystore` may be tweaked or used as is to generate the secrets and a CA:

```shell
myhost:Rembus.jl> bin/init_keystore [-n server_dns] [-i ip_address] [-k keystore_dir]
```

### options

- `-i` register an ip address that may be used to connect securely.
- `-n` DNS server name (default to `broker`).
- `-k` generated directory name containing the secret materials (default to `$HOME/.config/rembus/keystore`).
