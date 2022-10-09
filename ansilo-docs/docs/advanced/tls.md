---
sidebar_position: 3
---

# TLS

To enable TLS add the following to your `ansilo.yml`:

```yaml
networking:
  tls:
    # Path to PEM-encoded X509 certificate
    certificate: ${dir}/keys/certs.crt
    # Path to PEM-encoded PKCS #8 formatted private key
    private_key: ${dir}/keys/private.key
```

:::info
By enabling TLS in your config, it will enable TLS for both HTTP and Postgres connections.
:::