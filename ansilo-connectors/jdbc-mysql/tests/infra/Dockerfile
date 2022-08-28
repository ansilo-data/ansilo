FROM mysql-base

ENV MYSQL_ROOT_PASSWORD=ansilo_root

COPY ./sql/* /docker-entrypoint-initdb.d/

# Install https://github.com/TimeToogo/lazyprox
RUN curl --fail https://lazyprox.elliotlevin.dev/lazyprox-x86_64-unknown-linux-musl -o ~/lazyprox && \
    chmod +x ~/lazyprox && \
    mv ~/lazyprox /usr/local/bin/lazyprox

# Copy and run boot script
COPY ./boot.sh /boot.sh
ENTRYPOINT [ "/bin/bash", "/boot.sh" ]
