FROM mongo-base

ENV MONGO_INITDB_ROOT_USERNAME=ansilo_root
ENV MONGO_INITDB_ROOT_PASSWORD=plzbenice
ENV MONGO_INITDB_DATABASE=db

COPY ./init.d/* /docker-entrypoint-initdb.d/

# Install https://github.com/TimeToogo/lazyprox
RUN apt-get update && \
    apt-get install -y curl netcat && \
    curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
        chmod +x ~/lazyprox && \
        mv ~/lazyprox /usr/local/bin/lazyprox

# Copy and run boot script
COPY ./boot.sh /boot.sh
ENTRYPOINT [ "/bin/bash", "/boot.sh" ]
