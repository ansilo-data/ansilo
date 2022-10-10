---
sidebar_position: 4
---

# Custom CA

We use [UBI9](https://catalog.redhat.com/software/containers/ubi9/ubi/615bcf606feffc5384e8452e) as the base image for Ansilo.
Installing a custom CA can be done by make the following changes to your Dockerfile.


```sh
# Copy your CA bundle into the container
COPY your-ca-bundle.crt /etc/pki/ca-trust/source/anchors/

# Regenerate CA files for openssl, java, etc
USER root
RUN update-ca-trust
USER ansilo
```

:::info
To add a certificate in the simple PEM or DER file formats to the
list of CAs trusted on the system:

Copy it to the
        /etc/pki/ca-trust/source/anchors/
subdirectory, and run the
        update-ca-trust
command.

If your certificate is in the extended BEGIN TRUSTED file format,
then place it into the main source/ directory instead.
:::