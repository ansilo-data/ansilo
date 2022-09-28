FROM mssql-base

ENV ACCEPT_EULA=Y
ENV SA_PASSWORD="Ansilo_root[!]"

USER root

# Install https://github.com/TimeToogo/lazyprox
RUN apt-get update && \
    apt-get install -y curl netcat jq && \
    curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
    chmod +x ~/lazyprox && \
    mv ~/lazyprox /usr/local/bin/lazyprox

# Add sql init scripts
COPY ./sql/* /init-sql/
RUN chown -R mssql:root /init-sql/

# Copy and run boot script
COPY ./boot.sh /boot.sh

USER mssql
ENTRYPOINT [ "/bin/bash", "/boot.sh" ]
