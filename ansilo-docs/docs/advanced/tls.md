---
sidebar_position: 3
---

# TLS

In order to encrypt data in-transit you will need to configure TLS for your node.

### Step 1: Copy private key and certificate into your image

Add the following steps to your `Dockerfile`:

```dockerfile
# Create folder for keys
RUN mkdir -p keys
# Copy the PEM-encoded private key into the image
COPY /path/to/your/private.key ./keys
# Copy the PEM-encoded X509 certificate into the image
COPY /path/to/your/cert.crt ./keys
```

### Step 2: To enable TLS add the following to your `ansilo.yml`

```yaml
networking:
  tls:
    # Path to PEM-encoded X509 certificate
    certificate: ${dir}/keys/cert.crt
    # Path to PEM-encoded PKCS #8 formatted private key
    private_key: ${dir}/keys/private.key
```

:::info
By enabling TLS in your config, it will enable TLS for both HTTP and Postgres connections.
:::