FROM vault-base

# Cannot add cap IPC_LOCK on fargate
ENV VAULT_DISABLE_MLOCK=true
ENV SKIP_SETCAP=true
ENV VAULT_DEV_ROOT_TOKEN_ID=ansilo_test_root_token
ENV VAULT_ADDR=http://localhost:8200

USER root

# Install https://github.com/TimeToogo/lazyprox
RUN apk add bash curl netcat-openbsd jq && \
    curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
    chmod +x ~/lazyprox && \
    mv ~/lazyprox /usr/local/bin/lazyprox

# Add sql init scripts
COPY ./config/* /config/
RUN chown -R vault:vault /config/

# Copy and run boot script
COPY ./boot.sh /boot.sh

USER vault
ENTRYPOINT [ "/bin/bash", "/boot.sh" ]
