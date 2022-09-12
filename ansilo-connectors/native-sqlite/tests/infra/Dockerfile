FROM postgres-base

ENV POSTGRES_PASSWORD=ansilo_root
ENV POSTGRES_DB=postgres

COPY ./sql/* /docker-entrypoint-initdb.d/

# Install https://github.com/TimeToogo/lazyprox
RUN apt-get update && \
    apt-get install -y curl && \
    curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
        chmod +x ~/lazyprox && \
        mv ~/lazyprox /usr/local/bin/lazyprox

# Copy and run boot script
COPY ./boot.sh /boot.sh
ENTRYPOINT [ "/bin/bash", "/boot.sh" ]
