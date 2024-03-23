# Secure connections setup

Encrypted WebSocket and TLS connections need keys and certificates adhering to the following rules:

- The rembus broker private key is named `caronte.key`.
- The rembus signed certificate is named `caronte.crt`.
- The environment variable `REMBUS_KEYSTORE` define the directory where `caronte.key` and `caronte.crt` must be located.
- The full path of the certificate or the bundle containing the CA that signed `caronte.crt` must be specified with the standard env variable `HTTP_CA_BUNDLE`.

`$REMBUS_KEYSTORE/rembus-ca.crt` is the default value for `HTTP_CA_BUNDLE` if it is unset.

## Secrets materials: quick bootstrapping  

The utility script `init keystore` may be tweaked or used as is to generate the secrets and a CA:

```shell
myhost:Rembus.jl> bin/init_keystore [-n server_dns] [-i ip_address] [-k keystore_dir]
```

### options

- `-i` register an ip address that may be used to connect securely.
- `-n` DNS server name (default to `caronte`).
- `-k` generated directory name containing the secret materials (default to `$HOME/keystore`).
